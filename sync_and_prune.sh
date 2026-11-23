#!/usr/bin/env bash
# Hourly: copy compacted Parquet (ais_positions, ais_static, collector_health) to
# Google Drive with rclone, verify it with `rclone check`, and only then prune.
#
# Spec rules ("Project Summary.pdf", Stage 1, Verified Sync & Safe Pruning):
#   * NEVER delete local files by age alone.
#   * A Parquet file is deleted only if it is older than PRUNE_MIN_AGE_DAYS (7)
#     AND this run's `rclone check` confirmed the remote copy is identical
#     (size + MD5; both local disk and Google Drive provide MD5). Confirmed paths
#     are also appended to data/state/synced_verified.txt.
#   * A staging hour (data/staging/D/HH.sqlite) is deleted only if it is older than
#     PRUNE_MIN_AGE_DAYS, was compacted, and every Parquet file built from it has
#     been verified on the remote and pruned.
#   * Any local Parquet file older than UNSYNCED_ALERT_HOURS (48) that is not
#     verified raises an ALERT; the count goes to data/state/unsynced_48h.txt,
#     which the collector copies into every collector_health row.
#
# Environment (all optional):
#   AIS_DATA_DIR           data root (default: <this dir>/data)
#   RCLONE_REMOTE          rclone destination (default "gdrive:drk-vssl/ais").
#                          Set it to an empty string to disable sync; nothing is
#                          ever pruned without a verified remote copy.
#   PRUNE_MIN_AGE_DAYS     default 7
#   UNSYNCED_ALERT_HOURS   default 48
#   DRY_RUN=1              print what would be deleted, delete nothing
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${AIS_DATA_DIR:-$SCRIPT_DIR/data}"
STAGING_DIR="$DATA_DIR/staging"
PARQUET_DIR="$DATA_DIR/parquet"
STATE_DIR="$DATA_DIR/state"
RCLONE_REMOTE="${RCLONE_REMOTE-gdrive:drk-vssl/ais}"
PRUNE_MIN_AGE_DAYS="${PRUNE_MIN_AGE_DAYS:-7}"
UNSYNCED_ALERT_HOURS="${UNSYNCED_ALERT_HOURS:-48}"
DRY_RUN="${DRY_RUN:-0}"

REMOTE_DIR="$RCLONE_REMOTE/parquet"
LEDGER="$STATE_DIR/synced_verified.txt"
ALERT_FILE="$STATE_DIR/unsynced_48h.txt"
PRUNE_MIN_AGE_MIN=$((PRUNE_MIN_AGE_DAYS * 1440))
ALERT_AGE_MIN=$((UNSYNCED_ALERT_HOURS * 60))

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) sync_and_prune: $*"; }

remove() {
  if [[ "$DRY_RUN" == "1" ]]; then
    log "[dry-run] would delete $1"
  else
    rm -f -- "$1"
    log "deleted $1"
  fi
}

# Relative paths of local Parquet files, sorted for comm(1). Extra args go to find.
list_parquet() {
  (cd "$PARQUET_DIR" && find . -type f -name '*.parquet' "$@" | sed 's|^\./||' | LC_ALL=C sort)
}

mkdir -p "$STAGING_DIR" "$PARQUET_DIR" "$STATE_DIR"
touch "$LEDGER"

# Only one run at a time.
exec 9>"$STATE_DIR/sync.lock"
if command -v flock >/dev/null 2>&1; then
  flock -n 9 || { log "another run is in progress, exiting"; exit 0; }
fi

work="$(mktemp -d "$STATE_DIR/sync.XXXXXX")"
trap 'rm -rf "$work"' EXIT
verified="$work/verified.txt"
: >"$verified"

# --- 1. Copy + verify ---------------------------------------------------------
if [[ -n "$RCLONE_REMOTE" ]]; then
  log "copying $PARQUET_DIR -> $REMOTE_DIR"
  # 'copy' (never 'sync'): pruning local files must never delete remote ones.
  if rclone copy "$PARQUET_DIR" "$REMOTE_DIR" --checksum --exclude '*.tmp' \
       --transfers 4 --checkers 8 --log-level NOTICE; then
    log "copy finished"
  else
    log "copy FAILED; verifying whatever is already on the remote"
  fi

  # One-way check: local file must exist remotely with identical size + hash.
  # --match lists only the files that are identical; only those count as verified.
  if rclone check "$PARQUET_DIR" "$REMOTE_DIR" --one-way --exclude '*.tmp' \
       --match "$work/match.txt" --log-level ERROR; then
    log "check: every local file verified on remote"
  else
    log "check: some files are missing/different on remote (or the check failed)"
  fi
  [[ -f "$work/match.txt" ]] && LC_ALL=C sort -u "$work/match.txt" >"$verified"
  cat "$LEDGER" "$verified" | LC_ALL=C sort -u >"$work/ledger" && mv "$work/ledger" "$LEDGER"
  log "verified $(wc -l <"$verified" | tr -d ' ') file(s) this run"
else
  log "RCLONE_REMOTE is empty; skipping sync. Nothing will be pruned."
fi

# --- 2. Prune verified Parquet older than PRUNE_MIN_AGE_DAYS -------------------
list_parquet -mmin "+$PRUNE_MIN_AGE_MIN" >"$work/old.txt"
LC_ALL=C comm -12 "$work/old.txt" "$verified" >"$work/prune.txt"
LC_ALL=C comm -23 "$work/old.txt" "$verified" >"$work/keep.txt"
while IFS= read -r rel; do
  remove "$PARQUET_DIR/$rel"
done <"$work/prune.txt"
if [[ -s "$work/keep.txt" ]]; then
  log "keeping $(wc -l <"$work/keep.txt" | tr -d ' ') old Parquet file(s): not verified on remote"
fi

# --- 3. Prune staging hours whose outputs are verified + pruned ----------------
(cd "$STAGING_DIR" && find . -type f -name '*.sqlite' -mmin "+$PRUNE_MIN_AGE_MIN" | sed 's|^\./||' | LC_ALL=C sort) \
  >"$work/staging_old.txt"
while IFS= read -r rel; do
  key="${rel%.sqlite}"
  day="${key%/*}"
  hour="${key#*/}"
  if ! grep -qxF "$key" "$STATE_DIR/compacted.txt" 2>/dev/null; then
    log "keeping staging $rel: not compacted"
    continue
  fi
  ok=1
  for out in "ais_positions/date=$day/$hour.parquet" \
             "collector_health/date=$day/$hour.parquet" \
             "ais_static/date=$day/ais_static.parquet"; do
    if [[ -e "$PARQUET_DIR/$out" ]] || ! grep -qxF "$out" "$LEDGER"; then
      ok=0
      break
    fi
  done
  if [[ "$ok" == "1" ]]; then
    remove "$STAGING_DIR/$rel"
    for side in -wal -shm; do
      [[ -e "$STAGING_DIR/$rel$side" ]] && remove "$STAGING_DIR/$rel$side"
    done
  fi
done <"$work/staging_old.txt"

if [[ "$DRY_RUN" != "1" ]]; then
  find "$PARQUET_DIR" "$STAGING_DIR" -mindepth 1 -type d -empty -delete
fi

# --- 4. Unsynced > UNSYNCED_ALERT_HOURS alert ----------------------------------
list_parquet -mmin "+$ALERT_AGE_MIN" >"$work/stale.txt"
LC_ALL=C comm -23 "$work/stale.txt" "$verified" >"$work/unsynced.txt"
unsynced="$(wc -l <"$work/unsynced.txt" | tr -d ' ')"
echo "$unsynced" >"$ALERT_FILE.tmp" && mv "$ALERT_FILE.tmp" "$ALERT_FILE"
if [[ "$unsynced" -gt 0 ]]; then
  log "ALERT: $unsynced Parquet file(s) older than ${UNSYNCED_ALERT_HOURS}h are not verified on remote, e.g.:"
  head -n 10 "$work/unsynced.txt" | sed 's/^/    /'
fi

log "done"
