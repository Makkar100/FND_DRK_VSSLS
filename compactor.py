#!/usr/bin/env python3
"""Hourly compactor: SQLite staging hours -> ZSTD Parquet (spec Stage 1 + section 4).

For each finished staging hour data/staging/YYYY-MM-DD/HH.sqlite it writes

    data/parquet/ais_positions/date=YYYY-MM-DD/HH.parquet
        ts_utc, mmsi, lat, lon, sog_kn, cog_deg, true_heading, nav_status, h3_r7
    data/parquet/collector_health/date=YYYY-MM-DD/HH.parquet
        ts_utc, connection_state, pings_received, unique_mmsis, buffer_rows,
        disconnect_reason, reconnect_attempts, unsynced_files_48h
    data/parquet/ais_static/date=YYYY-MM-DD/ais_static.parquet   (deduplicated daily)
        ts_utc, mmsi, imo, vessel_name, callsign, ship_type, dim_a, dim_b, dim_c,
        dim_d, length_m, beam_m, draught_m, destination, flag_mid

ais_static keeps one row per distinct (mmsi + attributes) per day, stamped with
the first time it was seen, so draught/destination changes are preserved.

Every file is written to a .tmp name and atomically renamed. Compacted hours are
recorded in data/state/compacted.txt so reruns are cheap and idempotent.
Run it from a systemd timer (see deploy/).
"""
from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import h3
import pyarrow as pa
import pyarrow.parquet as pq

import config

log = logging.getLogger("ais.compactor")

TS = pa.timestamp("ms", tz="UTC")

POSITIONS_SCHEMA = pa.schema(
    [
        ("ts_utc", TS),
        ("mmsi", pa.int64()),
        ("lat", pa.float64()),
        ("lon", pa.float64()),
        ("sog_kn", pa.float64()),
        ("cog_deg", pa.float64()),
        ("true_heading", pa.int32()),
        ("nav_status", pa.int32()),
        ("h3_r7", pa.string()),
    ]
)

STATIC_SCHEMA = pa.schema(
    [
        ("ts_utc", TS),
        ("mmsi", pa.int64()),
        ("imo", pa.int64()),
        ("vessel_name", pa.string()),
        ("callsign", pa.string()),
        ("ship_type", pa.int32()),
        ("dim_a", pa.int32()),
        ("dim_b", pa.int32()),
        ("dim_c", pa.int32()),
        ("dim_d", pa.int32()),
        ("length_m", pa.int32()),
        ("beam_m", pa.int32()),
        ("draught_m", pa.float64()),
        ("destination", pa.string()),
        ("flag_mid", pa.string()),
    ]
)

HEALTH_SCHEMA = pa.schema(
    [
        ("ts_utc", TS),
        ("connection_state", pa.string()),
        ("pings_received", pa.int64()),
        ("unique_mmsis", pa.int64()),
        ("buffer_rows", pa.int64()),
        ("disconnect_reason", pa.string()),
        ("reconnect_attempts", pa.int64()),
        ("unsynced_files_48h", pa.int64()),
    ]
)

POSITION_TYPES = {"PositionReport", "StandardClassBPositionReport", "ExtendedClassBPositionReport"}
STATIC_TYPES = {"ShipStaticData"}

STATE_FILE = config.STATE_DIR / "compacted.txt"

# aisstream MetaData.time_utc looks like "2026-11-22 18:22:32.318353123 +0000 UTC";
# our own recv_ts is ISO 8601. Both are UTC.
_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:\.(\d+))?")


def parse_ts(s: Any) -> Optional[datetime]:
    if not isinstance(s, str):
        return None
    m = _TS_RE.match(s)
    if not m:
        return None
    frac = (m.group(3) or "0")[:6].ljust(6, "0")
    return datetime.strptime(f"{m.group(1)} {m.group(2)}.{frac}", "%Y-%m-%d %H:%M:%S.%f").replace(
        tzinfo=timezone.utc
    )


def _num(v: Any, cast=float):
    if v is None or v == "" or isinstance(v, bool):
        return None
    try:
        return cast(v)
    except (TypeError, ValueError):
        return None


def _text(v: Any) -> Optional[str]:
    if not isinstance(v, str):
        return None
    v = v.replace("@", " ").strip()  # AIS pads text fields with '@'
    return v or None


def _positive(v: Optional[float]):
    return v if v is not None and v > 0 else None


def flag_mid(mmsi: Optional[int]) -> Optional[str]:
    """Maritime Identification Digits for ship-station MMSIs (MIDxxxxxx, MID 2xx-7xx)."""
    if mmsi is None:
        return None
    s = str(mmsi)
    return s[:3] if len(s) == 9 and s[0] in "234567" else None


def _common(recv_ts: str, msg: Dict[str, Any], body: Dict[str, Any]):
    meta = msg.get("MetaData") or {}
    ts = parse_ts(meta.get("time_utc")) or parse_ts(recv_ts)
    mmsi = _num(meta.get("MMSI") or body.get("UserID"), int)
    return meta, ts, mmsi


def position_row(recv_ts: str, msg: Dict[str, Any], body: Dict[str, Any]) -> Dict[str, Any]:
    meta, ts, mmsi = _common(recv_ts, msg, body)
    lat = _num(body.get("Latitude", meta.get("latitude")))
    lon = _num(body.get("Longitude", meta.get("longitude")))
    if lat is not None and not -90 <= lat <= 90:
        lat = None  # 91 = not available
    if lon is not None and not -180 <= lon <= 180:
        lon = None  # 181 = not available
    sog = _num(body.get("Sog"))
    cog = _num(body.get("Cog"))
    hdg = _num(body.get("TrueHeading"), int)
    return {
        "ts_utc": ts,
        "mmsi": mmsi,
        "lat": lat,
        "lon": lon,
        "sog_kn": sog if sog is not None and sog < 102.3 else None,   # 102.3 = n/a
        "cog_deg": cog if cog is not None and cog < 360 else None,    # 360 = n/a
        "true_heading": hdg if hdg is not None and hdg < 360 else None,  # 511 = n/a
        "nav_status": _num(body.get("NavigationalStatus"), int),
        "h3_r7": h3.latlng_to_cell(lat, lon, config.H3_RESOLUTION)
        if lat is not None and lon is not None
        else None,
    }


def static_row(recv_ts: str, msg: Dict[str, Any], body: Dict[str, Any]) -> Dict[str, Any]:
    meta, ts, mmsi = _common(recv_ts, msg, body)
    dim = body.get("Dimension") or {}
    a, b, c, d = (_num(dim.get(k), int) for k in ("A", "B", "C", "D"))
    length = (a or 0) + (b or 0)
    beam = (c or 0) + (d or 0)
    return {
        "ts_utc": ts,
        "mmsi": mmsi,
        "imo": _positive(_num(body.get("ImoNumber"), int)),
        "vessel_name": _text(body.get("Name")) or _text(meta.get("ShipName")),
        "callsign": _text(body.get("CallSign")),
        "ship_type": _num(body.get("Type"), int),
        "dim_a": a,
        "dim_b": b,
        "dim_c": c,
        "dim_d": d,
        "length_m": length or None,
        "beam_m": beam or None,
        "draught_m": _positive(_num(body.get("MaximumStaticDraught"))),
        "destination": _text(body.get("Destination")),
        "flag_mid": flag_mid(mmsi),
    }


def write_parquet(rows: List[Dict[str, Any]], schema: pa.Schema, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".tmp")
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), tmp, compression="zstd")
    os.replace(tmp, out)  # atomic, so sync never sees a half-written file


def merge_static_day(rows: List[Dict[str, Any]], out: Path) -> int:
    """Merge new static rows into the day's file, one row per distinct record (earliest ts)."""
    existing = pq.read_table(out).to_pylist() if out.exists() else []
    if not rows and out.exists():
        return len(existing)
    attrs = [f.name for f in STATIC_SCHEMA if f.name != "ts_utc"]
    best: Dict[tuple, Dict[str, Any]] = {}
    for r in existing + rows:
        if r["mmsi"] is None:
            continue
        k = tuple(r[a] for a in attrs)
        cur = best.get(k)
        if cur is None or (r["ts_utc"] is not None and (cur["ts_utc"] is None or r["ts_utc"] < cur["ts_utc"])):
            best[k] = r
    merged = sorted(best.values(), key=lambda r: (r["mmsi"], r["ts_utc"] or datetime.min.replace(tzinfo=timezone.utc)))
    write_parquet(merged, STATIC_SCHEMA, out)
    return len(merged)


def compact_hour(src: Path) -> Dict[str, int]:
    date_str, hour_str = src.parent.name, src.stem
    con = sqlite3.connect(src, timeout=30)
    try:
        msgs = con.execute("SELECT recv_ts, msg_type, raw FROM messages ORDER BY rowid").fetchall()
        health = con.execute(
            "SELECT ts_utc, connection_state, pings_received, unique_mmsis, buffer_rows, "
            "disconnect_reason, reconnect_attempts, unsynced_files_48h "
            "FROM collector_health ORDER BY ts_utc"
        ).fetchall()
    finally:
        con.close()

    positions: List[Dict[str, Any]] = []
    statics: List[Dict[str, Any]] = []
    bad = 0
    for recv_ts, mtype, raw in msgs:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            bad += 1
            continue
        mtype = mtype or msg.get("MessageType")
        body = (msg.get("Message") or {}).get(mtype) or {}
        if mtype in POSITION_TYPES:
            positions.append(position_row(recv_ts, msg, body))
        elif mtype in STATIC_TYPES:
            statics.append(static_row(recv_ts, msg, body))

    health_rows = [
        dict(zip(HEALTH_SCHEMA.names, (parse_ts(r[0]),) + tuple(r[1:]))) for r in health
    ]

    part = f"date={date_str}"
    write_parquet(positions, POSITIONS_SCHEMA, config.PARQUET_DIR / "ais_positions" / part / f"{hour_str}.parquet")
    write_parquet(health_rows, HEALTH_SCHEMA, config.PARQUET_DIR / "collector_health" / part / f"{hour_str}.parquet")
    n_static_day = merge_static_day(statics, config.PARQUET_DIR / "ais_static" / part / "ais_static.parquet")
    return {
        "positions": len(positions),
        "static": len(statics),
        "static_day": n_static_day,
        "health": len(health_rows),
        "bad": bad,
    }


def load_state() -> set:
    if not STATE_FILE.exists():
        return set()
    return {l.strip() for l in STATE_FILE.read_text().splitlines() if l.strip()}


def mark_done(key: str) -> None:
    with open(STATE_FILE, "a", encoding="utf-8") as fh:
        fh.write(key + "\n")


def is_finished(path: Path, now: datetime) -> bool:
    """A staging hour is finished once its hour has ended and the grace period passed."""
    try:
        start = datetime.strptime(f"{path.parent.name} {path.stem}", "%Y-%m-%d %H").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return False
    return (now - start).total_seconds() / 60 >= 60 + config.COMPACT_GRACE_MINUTES


def main() -> None:
    config.ensure_dirs()
    done = load_state()
    now = datetime.now(timezone.utc)
    started = time.monotonic()
    n_files = n_rows = 0

    for src in sorted(config.STAGING_DIR.glob("*/*.sqlite")):
        key = f"{src.parent.name}/{src.stem}"
        if key in done or not is_finished(src, now):
            continue
        try:
            stats = compact_hour(src)
        except Exception:  # noqa: BLE001
            log.exception("failed to compact %s", src)
            continue
        mark_done(key)
        n_files += 1
        n_rows += stats["positions"] + stats["static"]
        log.info("compacted %s %s", key, stats)

    log.info("done: %d hours, %d message rows in %.1fs", n_files, n_rows, time.monotonic() - started)


if __name__ == "__main__":
    logging.basicConfig(
        level=config.LOG_LEVEL,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    main()
