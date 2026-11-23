"""Central configuration for the AIS pipeline (Phase 0 / Stage 1).

Values follow "Project Summary.pdf" (section 2, Unified Global Constants, and
section 5, Stage 1). Every value can be overridden with an environment variable
of the same name, so the systemd units (via /etc/ais/ais.env) can change
behaviour without code edits.
"""
from __future__ import annotations

import os
from pathlib import Path


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, str(default)))


# --- Source -----------------------------------------------------------------
# aisstream.io free websocket feed. Get a key at https://aisstream.io
# Licence: no redistribution, no public live map, no commercial use (spec 1.5).
AISSTREAM_URL = _env("AISSTREAM_URL", "wss://stream.aisstream.io/v0/stream")
AISSTREAM_API_KEY = _env("AISSTREAM_API_KEY", "")

# Bounding boxes as [[[lat_min, lon_min], [lat_max, lon_max]], ...].
# Default: Phase 1 AOI, Eastern OPL (Malaysia / Singapore).
# Future expansion (spec 1.4b), e.g.
#   Gulf of Oman    [[23.8, 56.0], [26.6, 58.5]]
#   Gulf of Finland [[59.2, 23.5], [60.5, 29.0]]
AIS_BBOXES_JSON = _env("AIS_BBOXES", "[[[1.2, 104.0], [2.6, 105.5]]]")

# Message types to subscribe to (spec Stage 1). Empty string = all types.
AIS_MESSAGE_TYPES = [
    t.strip()
    for t in _env("AIS_MESSAGE_TYPES", "PositionReport,ShipStaticData").split(",")
    if t.strip()
]

# --- Storage ----------------------------------------------------------------
DATA_DIR = Path(_env("AIS_DATA_DIR", str(Path(__file__).resolve().parent / "data")))
# Durable micro-batch staging: one SQLite (WAL) file per UTC hour,
# data/staging/YYYY-MM-DD/HH.sqlite, holding raw messages + collector_health rows.
STAGING_DIR = DATA_DIR / "staging"
# Compacted ZSTD Parquet: ais_positions/, ais_static/, collector_health/
PARQUET_DIR = DATA_DIR / "parquet"
STATE_DIR = DATA_DIR / "state"        # compactor + sync bookkeeping

# --- Collector (spec section 2 + Stage 1) ------------------------------------
WS_RECV_TIMEOUT_SEC = _env_float("WS_RECV_TIMEOUT_SEC", 60.0)     # watchdog
MICRO_FLUSH_SEC = _env_float("MICRO_FLUSH_SEC", 300.0)            # flush every 5 min...
MICRO_FLUSH_ROWS = _env_int("MICRO_FLUSH_ROWS", 10_000)           # ...or 10,000 rows
RECONNECT_MIN_SECONDS = _env_float("AIS_RECONNECT_MIN_SECONDS", 2.0)
RECONNECT_MAX_SECONDS = _env_float("AIS_RECONNECT_MAX_SECONDS", 120.0)
HEARTBEAT_SEC = _env_float("AIS_HEARTBEAT_SEC", 60.0)             # collector_health row

# --- Compactor ----------------------------------------------------------------
H3_RESOLUTION = _env_int("H3_RESOLUTION", 7)
# Staging hour files are only compacted once the hour has ended plus this grace
# period (must exceed MICRO_FLUSH_SEC so the last micro-batch has landed).
COMPACT_GRACE_MINUTES = _env_int("AIS_COMPACT_GRACE_MINUTES", 10)

# --- Sync & prune (used by sync_and_prune.sh, listed here for reference) ----
# RCLONE_REMOTE         default "gdrive:drk-vssl/ais" (empty = no sync, no pruning)
# PRUNE_MIN_AGE_DAYS    default 7  (only files verified by `rclone check` are pruned)
# UNSYNCED_ALERT_HOURS  default 48 (count written to state/unsynced_48h.txt and
#                                   copied into every collector_health row)
SYNC_ALERT_FILE = STATE_DIR / "unsynced_48h.txt"

LOG_LEVEL = _env("AIS_LOG_LEVEL", "INFO")


def ensure_dirs() -> None:
    for d in (STAGING_DIR, PARQUET_DIR, STATE_DIR):
        d.mkdir(parents=True, exist_ok=True)
