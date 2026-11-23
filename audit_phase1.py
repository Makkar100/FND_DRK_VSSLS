#!/usr/bin/env python3
"""Phase 1 audit of the AIS Parquet data in ./data/ais (DRK VSSL).

What it does:
  1. Shows what each subfolder inside data/ais holds.
  2. Shows the schema and row count of every hourly .parquet file
     (ais_positions, collector_health) and of ais_static.parquet.
  3. Reports total rows, unique MMSIs, earliest/latest timestamps and
     min/max latitude/longitude.
  4. Flags malformed coordinates, null timestamps and other bad rows.

Layout written by compactor.py (Phase 0):
  data/ais/parquet/ais_positions/date=YYYY-MM-DD/HH.parquet
  data/ais/parquet/collector_health/date=YYYY-MM-DD/HH.parquet
  data/ais/parquet/ais_static/date=YYYY-MM-DD/ais_static.parquet

Usage:
  python3 audit_phase1.py                 # audits ./data/ais next to this script
  python3 audit_phase1.py /path/to/ais    # audits another folder

Needs:  pip3 install duckdb
Exit code is 0 when no problems are found, 1 when any check flags rows.
"""
from __future__ import annotations

import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

try:
    import duckdb
except ImportError:
    sys.exit("duckdb is not installed. Run:  python3 -m pip install duckdb")

# Phase 1 area of interest (Eastern OPL), same as AIS_BBOXES in config.py.
AOI_LAT_MIN, AOI_LAT_MAX = 1.2, 2.6
AOI_LON_MIN, AOI_LON_MAX = 104.0, 105.5
AOI_BUFFER_DEG = 0.1  # AISStream can return points just outside the box

TABLES = ("ais_positions", "ais_static", "collector_health")


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def header(title: str) -> None:
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def human_size(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def sql_list(paths: list[Path]) -> str:
    """Turn paths into a DuckDB list literal, escaping single quotes."""
    return "[" + ", ".join("'" + str(p).replace("'", "''") + "'" for p in paths) + "]"


def scan(paths: list[Path]) -> str:
    return f"read_parquet({sql_list(paths)}, filename = true, union_by_name = true)"


def table_files(ais_dir: Path, table: str) -> list[Path]:
    return sorted((ais_dir / "parquet" / table).glob("date=*/*.parquet"))


def columns_of(con: duckdb.DuckDBPyConnection, paths: list[Path]) -> set[str]:
    rows = con.execute(f"DESCRIBE SELECT * FROM {scan(paths)}").fetchall()
    return {r[0] for r in rows}


# --------------------------------------------------------------------------- #
# 1. folder inventory
# --------------------------------------------------------------------------- #
def guess_table(con: duckdb.DuckDBPyConnection, f: Path) -> str:
    try:
        cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {scan([f])}").fetchall()}
    except duckdb.Error as e:
        return f"UNREADABLE ({e.__class__.__name__})"
    if {"lat", "lon", "sog_kn"} <= cols:
        return "AIS position reports"
    if {"vessel_name", "imo"} <= cols:
        return "AIS static / voyage data"
    if "connection_state" in cols:
        return "collector heartbeat (1-min health)"
    return "unknown parquet"


def inventory(con: duckdb.DuckDBPyConnection, ais_dir: Path) -> None:
    header(f"1. FOLDERS INSIDE {ais_dir}")
    dirs = [ais_dir] + sorted(p for p in ais_dir.rglob("*") if p.is_dir())
    for d in dirs:
        files = [f for f in d.iterdir() if f.is_file() and f.name != ".DS_Store"]
        subdirs = [s.name for s in d.iterdir() if s.is_dir()]
        rel = d.relative_to(ais_dir.parent.parent)
        print(f"\n{rel}/")
        if subdirs:
            print(f"  subfolders : {', '.join(sorted(subdirs))}")
        if not files:
            if not subdirs:
                print("  (empty)")
            continue
        exts = Counter(f.suffix or "(none)" for f in files)
        size = sum(f.stat().st_size for f in files)
        print(f"  files      : {len(files)}  ({', '.join(f'{n} {e}' for e, n in exts.items())}), {human_size(size)}")
        pq_files = sorted(f for f in files if f.suffix == ".parquet")
        if pq_files:
            kinds = Counter(guess_table(con, f) for f in pq_files)
            print(f"  contains   : {', '.join(kinds)}")
            names = [f.stem for f in pq_files]
            print(f"  file names : {names[0]} .. {names[-1]}" if len(names) > 1 else f"  file name  : {names[0]}")
            hours = sorted(int(n) for n in names if n.isdigit())
            if hours:
                missing = sorted(set(range(hours[0], hours[-1] + 1)) - set(hours))
                print(f"  UTC hours  : {hours[0]:02d}-{hours[-1]:02d}"
                      + (f"  MISSING HOURS: {', '.join(f'{h:02d}' for h in missing)}" if missing else "  (no gaps)"))


# --------------------------------------------------------------------------- #
# 2. per-file schema and row counts
# --------------------------------------------------------------------------- #
def schemas_and_counts(con: duckdb.DuckDBPyConnection, ais_dir: Path) -> None:
    header("2. SCHEMA AND ROW COUNT PER FILE")
    for table in TABLES:
        files = table_files(ais_dir, table)
        print(f"\n--- {table}: {len(files)} file(s)")
        if not files:
            print("  NO FILES FOUND")
            continue
        schema = con.execute(f"DESCRIBE SELECT * FROM read_parquet({sql_list(files[:1])})").fetchall()
        print("  schema (from first file):")
        for name, dtype, *_ in schema:
            note = "   (virtual: from date=... folder name)" if name == "date" else ""
            print(f"    {name:<22} {dtype}{note}")
        first_sig = [(r[0], r[1]) for r in schema]
        total = 0
        print("  rows per file:")
        for f in files:
            n = con.execute(f"SELECT count(*) FROM read_parquet({sql_list([f])})").fetchone()[0]
            sig = [(r[0], r[1]) for r in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet({sql_list([f])})").fetchall()]
            note = "" if sig == first_sig else "   <-- SCHEMA DIFFERS FROM FIRST FILE"
            print(f"    {f.parent.name}/{f.name:<20} {n:>10,}{note}")
            total += n
        print(f"    {'TOTAL':<31} {total:>10,}")


# --------------------------------------------------------------------------- #
# 3. summary stats
# --------------------------------------------------------------------------- #
def summary(con: duckdb.DuckDBPyConnection, ais_dir: Path) -> None:
    header("3. SUMMARY")
    for table in TABLES:
        files = table_files(ais_dir, table)
        if not files:
            continue
        cols = columns_of(con, files)
        parts = ["count(*) AS rows"]
        if "mmsi" in cols:
            parts.append("count(DISTINCT mmsi) AS unique_mmsis")
        if "ts_utc" in cols:
            parts += ["min(ts_utc) AS earliest_ts", "max(ts_utc) AS latest_ts"]
        if {"lat", "lon"} <= cols:
            parts += ["min(lat) AS min_lat", "max(lat) AS max_lat",
                      "min(lon) AS min_lon", "max(lon) AS max_lon"]
        rel = con.execute(f"SELECT {', '.join(parts)} FROM {scan(files)}")
        names = [d[0] for d in rel.description]
        values = rel.fetchone()
        print(f"\n--- {table}")
        for k, v in zip(names, values):
            if isinstance(v, int):
                v = f"{v:,}"
            print(f"  {k:<14} {v}")
        if "connection_state" in cols:
            states = con.execute(
                f"SELECT connection_state, count(*) FROM {scan(files)} GROUP BY 1 ORDER BY 2 DESC"
            ).fetchall()
            print("  connection states: " + ", ".join(f"{s}={n:,}" for s, n in states))


# --------------------------------------------------------------------------- #
# 4. data quality flags
# --------------------------------------------------------------------------- #
def quality(con: duckdb.DuckDBPyConnection, ais_dir: Path) -> int:
    header("4. DATA QUALITY FLAGS")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    lat_lo, lat_hi = AOI_LAT_MIN - AOI_BUFFER_DEG, AOI_LAT_MAX + AOI_BUFFER_DEG
    lon_lo, lon_hi = AOI_LON_MIN - AOI_BUFFER_DEG, AOI_LON_MAX + AOI_BUFFER_DEG

    # (label, SQL condition, needed columns, is_error)
    # is_error=False means "warning": worth knowing but not malformed data.
    checks = [
        ("null timestamp", "ts_utc IS NULL", {"ts_utc"}, True),
        ("timestamp in the future", f"ts_utc > TIMESTAMPTZ '{now}+00'", {"ts_utc"}, True),
        ("timestamp before 2020", "ts_utc < TIMESTAMPTZ '2020-01-01 00:00:00+00'", {"ts_utc"}, True),
        ("ts hour != file hour",
         "regexp_extract(filename, '(\\d{2})\\.parquet$', 1) <> '' AND "
         "hour(timezone('UTC', ts_utc)) <> "
         "TRY_CAST(regexp_extract(filename, '(\\d{2})\\.parquet$', 1) AS INTEGER)",
         {"ts_utc", "filename"}, False),
        ("null mmsi", "mmsi IS NULL", {"mmsi"}, True),
        ("mmsi not 9 digits", "mmsi IS NOT NULL AND (mmsi < 100000000 OR mmsi > 999999999)", {"mmsi"}, False),
        ("null lat or lon", "lat IS NULL OR lon IS NULL", {"lat", "lon"}, True),
        ("NaN lat or lon", "isnan(lat) OR isnan(lon)", {"lat", "lon"}, True),
        ("lat outside -90..90", "lat < -90 OR lat > 90", {"lat"}, True),
        ("lon outside -180..180", "lon < -180 OR lon > 180", {"lon"}, True),
        ("AIS 'not available' 91/181", "lat = 91 OR lon = 181", {"lat", "lon"}, True),
        ("null island (0, 0)", "lat = 0 AND lon = 0", {"lat", "lon"}, True),
        (f"outside AOI (+{AOI_BUFFER_DEG} deg)",
         f"lat IS NOT NULL AND lon IS NOT NULL AND "
         f"(lat NOT BETWEEN {lat_lo} AND {lat_hi} OR lon NOT BETWEEN {lon_lo} AND {lon_hi})",
         {"lat", "lon"}, False),
        ("lat/lon but no h3_r7", "lat IS NOT NULL AND lon IS NOT NULL AND h3_r7 IS NULL",
         {"lat", "lon", "h3_r7"}, True),
        ("sog_kn < 0 or > 102.2", "sog_kn < 0 OR sog_kn > 102.2", {"sog_kn"}, False),
        ("cog_deg < 0 or > 360", "cog_deg < 0 OR cog_deg > 360", {"cog_deg"}, False),
    ]

    errors = 0
    warnings = 0
    for table in TABLES:
        files = table_files(ais_dir, table)
        if not files:
            continue
        cols = columns_of(con, files)
        total = con.execute(f"SELECT count(*) FROM {scan(files)}").fetchone()[0]
        print(f"\n--- {table} ({total:,} rows)")
        for label, cond, need, is_error in checks:
            if not need <= cols:
                continue
            n = con.execute(f"SELECT count(*) FROM {scan(files)} WHERE {cond}").fetchone()[0]
            if n == 0:
                print(f"  OK    {label}")
                continue
            tag = "FLAG " if is_error else "WARN "
            pct = 100.0 * n / total if total else 0.0
            print(f"  {tag} {label}: {n:,} rows ({pct:.2f}%)")
            show = [c for c in ("ts_utc", "mmsi", "lat", "lon") if c in cols]
            sample = con.execute(
                f"SELECT {', '.join(show)}, regexp_extract(filename, '[^/\\\\]+/[^/\\\\]+$') AS file "
                f"FROM {scan(files)} WHERE {cond} LIMIT 3"
            ).fetchall()
            for row in sample:
                print("          e.g. " + ", ".join(f"{k}={v}" for k, v in zip(show + ["file"], row)))
            if is_error:
                errors += n
            else:
                warnings += n

        if "mmsi" in cols and "ts_utc" in cols and table == "ais_positions":
            dups = con.execute(
                f"SELECT count(*) - count(DISTINCT (mmsi, ts_utc, lat, lon)) FROM {scan(files)}"
            ).fetchone()[0]
            if dups:
                print(f"  WARN  exact duplicate pings (mmsi, ts, lat, lon): {dups:,}")
                warnings += dups
            else:
                print("  OK    exact duplicate pings")

    print()
    print(f"Result: {errors:,} flagged rows, {warnings:,} warning rows.")
    return errors


# --------------------------------------------------------------------------- #
def main() -> int:
    base = Path(__file__).resolve().parent
    ais_dir = Path(sys.argv[1]).expanduser().resolve() if len(sys.argv) > 1 else base / "data" / "ais"
    if not ais_dir.is_dir():
        print(f"Folder not found: {ais_dir}")
        return 2

    print(f"DRK VSSL Phase 1 audit   duckdb {duckdb.__version__}   "
          f"run {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC")
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")

    inventory(con, ais_dir)
    schemas_and_counts(con, ais_dir)
    summary(con, ais_dir)
    errors = quality(con, ais_dir)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
