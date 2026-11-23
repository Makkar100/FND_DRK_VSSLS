#!/usr/bin/env bash
# run_daily.sh - DRK VSSL Phase 1 daily pipeline.
#
#   1. Sync AIS Parquet from Google Drive   (rclone copy gdrive:drk-vssl/ais ./data/ais)
#   2. Audit the data                       (audit_phase1.py)
#   3. Detect dark gaps                     (detect_gaps_phase1.py)
#   4. Build the map                        (build_map_phase1.py)
#
# Usage:
#   ./run_daily.sh              # sync, then run the analysis
#   ./run_daily.sh --no-sync    # skip the download, re-run the analysis on local data
#
# Needs: python3 with duckdb folium h3 pandas pyarrow; rclone (only when syncing).
# Works with the stock macOS bash 3.2. Stops at the first failing step.

set -euo pipefail

# Always run from the folder this script lives in.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RCLONE_SRC="gdrive:drk-vssl/ais"
MAP_PATH="./outputs/opl_map_phase1.html"
DO_SYNC=1

usage() {
  sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'
}

for arg in "$@"; do
  case "$arg" in
    --no-sync) DO_SYNC=0 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $arg" >&2; usage >&2; exit 2 ;;
  esac
done

step() {
  echo
  echo "==> [$1/4] $2"
}

fail() {
  echo
  echo "FAILED: step $CURRENT_STEP did not finish. Fix the error above and re-run." >&2
}
CURRENT_STEP=0
trap 'rc=$?; if [ "$rc" -ne 0 ] && [ "$CURRENT_STEP" -gt 0 ]; then fail; fi' EXIT

START=$SECONDS
echo "DRK VSSL Phase 1 daily run - $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "Folder: $SCRIPT_DIR"

# Pre-flight: fail fast with a clear hint instead of a Python traceback.
if ! python3 -c "import duckdb, folium, h3, pandas, pyarrow" >/dev/null 2>&1; then
  echo "Missing Python packages. Run:  python3 -m pip install duckdb folium h3 pandas pyarrow" >&2
  exit 1
fi

CURRENT_STEP=1
if [ "$DO_SYNC" -eq 1 ]; then
  step 1 "Syncing $RCLONE_SRC -> ./data/ais"
  if ! command -v rclone >/dev/null 2>&1; then
    echo "rclone not found. Install it (brew install rclone) or run with --no-sync." >&2
    exit 1
  fi
  mkdir -p ./data/ais
  rclone copy "$RCLONE_SRC" ./data/ais --progress
else
  step 1 "Sync skipped (--no-sync), using local ./data/ais"
fi

CURRENT_STEP=2
step 2 "Auditing data (audit_phase1.py)"
python3 audit_phase1.py

CURRENT_STEP=3
step 3 "Detecting dark gaps (detect_gaps_phase1.py)"
python3 detect_gaps_phase1.py

CURRENT_STEP=4
step 4 "Building map (build_map_phase1.py)"
python3 build_map_phase1.py

CURRENT_STEP=0
ELAPSED=$((SECONDS - START))

echo
echo "==> Done: all 4 steps passed in ${ELAPSED}s"
if [ "$DO_SYNC" -eq 1 ]; then echo "    Sync:    ran"; else echo "    Sync:    skipped"; fi
if [ -f ./outputs/dark_events_phase1.csv ]; then
  # Data rows in the gap CSV (header excluded).
  GAPS=$(($(wc -l < ./outputs/dark_events_phase1.csv) - 1))
  echo "    Gaps:    $GAPS rows in ./outputs/dark_events_phase1.csv"
fi
if [ ! -f "$MAP_PATH" ]; then
  echo "Map file not found at $MAP_PATH" >&2
  exit 1
fi
echo "    Map:     $SCRIPT_DIR/outputs/opl_map_phase1.html"
