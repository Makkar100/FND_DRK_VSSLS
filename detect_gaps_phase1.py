#!/usr/bin/env python3
"""Phase 1B baseline dark-gap detector for DRK VSSL (DuckDB).

Reads the Phase 0 Parquet layout under ./data/ais/parquet/:
  ais_positions/date=YYYY-MM-DD/HH.parquet
  collector_health/date=YYYY-MM-DD/HH.parquet
  ais_static/date=YYYY-MM-DD/ais_static.parquet   (a flat ais_static/ais_static.parquet also works)

Steps:
  1. Collector outage guard: windows where connection_state is 'backoff' or
     'connecting', plus any stretch where the 1-minute heartbeat went missing.
  2. H3 coverage baseline per h3_r7 cell  -> outputs/h3_coverage_phase1.parquet
  3. Static join + hull filter (MIN_HULL_LENGTH_M). Vessels with no static
     record (or no length) are kept and tagged 'unknown_length'.
  4. Dark-gap detector: LAG/LEAD per mmsi ordered by ts_utc -> gap minutes,
     Haversine distance (nm), implied speed (kn). Flag gaps > GAP_MIN_MINUTES
     where the vessel was underway (sog_kn >= MIN_SOG_KN) before going quiet,
     after removing any minutes that overlap a collector outage.
  5. Anchorage mask: point-in-polygon test (pure SQL ray casting) of each gap's
     start and end point against masks/anchorages.geojson -> in_anchorage.
     gap_category = 'anchorage_drift' when an end point is in a drift zone
     (counts_as_drift = true) or the gap barely moved (distance_nm <
     DRIFT_MAX_DISTANCE_NM and implied_speed_kn < DRIFT_MAX_SPEED_KN);
     otherwise 'open_water_candidate'. OPL waiting zones are tagged but do not
     count as drift on their own, because OPL loitering is what we hunt.
  6. Writes outputs/dark_events_phase1.parquet and .csv, prints a summary.

Usage:
  python3 detect_gaps_phase1.py               # uses ./data/ais next to this script
  python3 detect_gaps_phase1.py /path/to/ais  # another data folder

Needs:  python3 -m pip install duckdb
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

try:
    import duckdb
except ImportError:
    sys.exit("duckdb is not installed. Run:  python3 -m pip install duckdb")

# --------------------------------------------------------------------------- #
# tunables
# --------------------------------------------------------------------------- #
MIN_HULL_LENGTH_M = 120        # keep vessels at least this long (known length)
GAP_MIN_MINUTES = 120          # candidate gap threshold
MIN_SOG_KN = 3.0               # "underway" before going silent
HEARTBEAT_MISSING_SEC = 180    # heartbeat silence longer than this = outage
OUTAGE_STATES = ("backoff", "connecting")
DRIFT_MAX_DISTANCE_NM = 3.0    # start->end displacement below this ...
DRIFT_MAX_SPEED_KN = 1.5       # ... and implied speed below this = drift
EARTH_RADIUS_NM = 3440.065

BASE_DIR = Path(__file__).resolve().parent
OUT_DIR = BASE_DIR / "outputs"
ANCHORAGE_MASK = BASE_DIR / "masks" / "anchorages.geojson"


def q(path: Path | str) -> str:
    """Quote a path or glob as a SQL string literal."""
    return "'" + str(path).replace("'", "''") + "'"


def find_files(root: Path, table: str) -> list[Path]:
    files = sorted((root / table).rglob("*.parquet"))
    if not files:
        sys.exit(f"No parquet files found under {root / table}")
    return files


def scan(files: list[Path]) -> str:
    lst = "[" + ", ".join(q(p) for p in files) + "]"
    return f"read_parquet({lst}, union_by_name = true, hive_partitioning = false)"


def require_columns(con, view: str, cols: list[str]) -> None:
    have = {r[0] for r in con.execute(f"DESCRIBE {view}").fetchall()}
    missing = [c for c in cols if c not in have]
    if missing:
        sys.exit(f"{view} is missing columns: {', '.join(missing)}")


def load_zones(con, path: Path) -> None:
    """Load GeoJSON (Multi)Polygons into `zones` and `zone_edges` tables.

    Each ring becomes a list of edges; a point is inside a polygon part when a
    ray cast east from it crosses that part's edges an odd number of times
    (even-odd rule, so holes work too). Coordinates are [lon, lat].
    """
    if not path.is_file():
        sys.exit(f"Anchorage mask not found: {path}")
    features = json.loads(path.read_text()).get("features", [])

    zones, edges = [], []
    for i, feat in enumerate(features):
        props = feat.get("properties") or {}
        geom = feat.get("geometry") or {}
        zone_id = str(props.get("zone_id") or f"zone_{i}")
        if geom.get("type") == "Polygon":
            parts = [geom["coordinates"]]
        elif geom.get("type") == "MultiPolygon":
            parts = geom["coordinates"]
        else:
            print(f"warning: skipping {zone_id}: geometry {geom.get('type')} is not a polygon")
            continue
        zones.append((zone_id, props.get("name"), props.get("zone_type"),
                      bool(props.get("counts_as_drift", True))))
        for part_no, rings in enumerate(parts):
            pts = [pt for ring in rings for pt in ring]
            lon_lo, lon_hi = min(p[0] for p in pts), max(p[0] for p in pts)
            lat_lo, lat_hi = min(p[1] for p in pts), max(p[1] for p in pts)
            for ring in rings:
                for (x1, y1), (x2, y2) in zip(ring, ring[1:] + ring[:1]):
                    if y1 != y2:  # horizontal edges never cross an eastward ray
                        edges.append((zone_id, part_no, x1, y1, x2, y2, lat_lo, lat_hi, lon_lo, lon_hi))
    if not zones:
        sys.exit(f"No polygon features in {path}")

    con.execute("CREATE TABLE zones (zone_id VARCHAR, name VARCHAR, zone_type VARCHAR, counts_as_drift BOOLEAN)")
    con.execute("""CREATE TABLE zone_edges (zone_id VARCHAR, part INTEGER,
                   x1 DOUBLE, y1 DOUBLE, x2 DOUBLE, y2 DOUBLE,
                   lat_lo DOUBLE, lat_hi DOUBLE, lon_lo DOUBLE, lon_hi DOUBLE)""")
    con.executemany("INSERT INTO zones VALUES (?, ?, ?, ?)", zones)
    con.executemany("INSERT INTO zone_edges VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", edges)


def zone_hits(points: str) -> str:
    """SQL: (pid, zone_id, counts_as_drift) for every point inside a zone.

    `points` is a relation with columns pid, lat, lon.
    """
    return f"""
        SELECT p.pid, e.zone_id, any_value(z.counts_as_drift) AS counts_as_drift
        FROM {points} p
        JOIN zone_edges e
          ON p.lat BETWEEN e.lat_lo AND e.lat_hi
         AND p.lon BETWEEN e.lon_lo AND e.lon_hi
         AND (e.y1 > p.lat) <> (e.y2 > p.lat)
         AND p.lon < e.x1 + (p.lat - e.y1) * (e.x2 - e.x1) / (e.y2 - e.y1)
        JOIN zones z USING (zone_id)
        GROUP BY p.pid, e.zone_id, e.part
        HAVING count(*) % 2 = 1
    """


def main() -> int:
    ais_dir = Path(sys.argv[1]).expanduser().resolve() if len(sys.argv) > 1 else BASE_DIR / "data" / "ais"
    pq = ais_dir / "parquet"
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")

    con.execute(f"CREATE VIEW positions AS SELECT * FROM {scan(find_files(pq, 'ais_positions'))}")
    con.execute(f"CREATE VIEW health    AS SELECT * FROM {scan(find_files(pq, 'collector_health'))}")
    con.execute(f"CREATE VIEW static_raw AS SELECT * FROM {scan(find_files(pq, 'ais_static'))}")
    require_columns(con, "positions", ["ts_utc", "mmsi", "lat", "lon", "sog_kn", "h3_r7"])
    require_columns(con, "health", ["ts_utc", "connection_state"])
    require_columns(con, "static_raw", ["mmsi", "length_m"])
    load_zones(con, ANCHORAGE_MASK)

    # Clean positions: drop rows with no time/id/coords, de-duplicate exact repeats.
    con.execute("""
        CREATE TABLE pos AS
        SELECT DISTINCT ts_utc, mmsi, lat, lon, sog_kn, h3_r7
        FROM positions
        WHERE ts_utc IS NOT NULL AND mmsi IS NOT NULL
          AND lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180
    """)

    # ------------------------------------------------------------------ #
    # 1. Collector outage windows
    # ------------------------------------------------------------------ #
    states = ", ".join(q(s) for s in OUTAGE_STATES)
    con.execute(f"""
        CREATE TABLE outages AS
        WITH hb AS (
            SELECT ts_utc,
                   lower(connection_state) AS state,
                   lag(ts_utc)  OVER (ORDER BY ts_utc) AS prev_ts,
                   lead(ts_utc) OVER (ORDER BY ts_utc) AS next_ts
            FROM health WHERE ts_utc IS NOT NULL
        ),
        raw AS (
            -- a bad heartbeat covers the time since the previous heartbeat
            -- up to the next one (the collector was not streaming)
            SELECT coalesce(prev_ts, ts_utc) AS w_start,
                   coalesce(next_ts, ts_utc) AS w_end,
                   state AS reason
            FROM hb WHERE state IN ({states})
            UNION ALL
            -- heartbeat went missing entirely (collector/VM down)
            SELECT prev_ts, ts_utc, 'heartbeat_missing'
            FROM hb
            WHERE prev_ts IS NOT NULL
              AND epoch(ts_utc) - epoch(prev_ts) > {HEARTBEAT_MISSING_SEC}
        ),
        ordered AS (
            SELECT *, max(w_end) OVER (ORDER BY w_start
                       ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS run_end
            FROM raw
        ),
        grp AS (
            SELECT *, sum(CASE WHEN run_end IS NULL OR w_start > run_end THEN 1 ELSE 0 END)
                      OVER (ORDER BY w_start ROWS UNBOUNDED PRECEDING) AS gid
            FROM ordered
        )
        SELECT gid AS outage_id,
               min(w_start) AS start_utc,
               max(w_end)   AS end_utc,
               round((epoch(max(w_end)) - epoch(min(w_start))) / 60.0, 1) AS minutes,
               string_agg(DISTINCT reason, ',') AS reasons
        FROM grp GROUP BY gid ORDER BY start_utc
    """)

    # ------------------------------------------------------------------ #
    # 2. H3 coverage baseline
    # ------------------------------------------------------------------ #
    con.execute("""
        CREATE TABLE h3_coverage AS
        SELECT h3_r7,
               count(*)                          AS pings,
               count(DISTINCT mmsi)              AS unique_mmsi,
               count(DISTINCT date_trunc('hour', ts_utc)) AS active_hours,
               round(count(*) / count(DISTINCT date_trunc('hour', ts_utc)), 2) AS pings_per_active_hour,
               round(avg(lat), 5)                AS mean_lat,
               round(avg(lon), 5)                AS mean_lon,
               round(avg(sog_kn), 2)             AS mean_sog_kn,
               min(ts_utc)                       AS first_seen_utc,
               max(ts_utc)                       AS last_seen_utc
        FROM pos WHERE h3_r7 IS NOT NULL
        GROUP BY h3_r7 ORDER BY pings DESC
    """)
    h3_out = OUT_DIR / "h3_coverage_phase1.parquet"
    con.execute(f"COPY h3_coverage TO {q(h3_out)} (FORMAT parquet, COMPRESSION zstd)")

    # ------------------------------------------------------------------ #
    # 3. Static join + hull filter (latest record per mmsi, prefer one with a length)
    # ------------------------------------------------------------------ #
    has_static_ts = "ts_utc" in {r[0] for r in con.execute("DESCRIBE static_raw").fetchall()}
    order_ts = "ts_utc DESC NULLS LAST" if has_static_ts else "mmsi"
    con.execute(f"""
        CREATE TABLE static AS
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, row_number() OVER (
                PARTITION BY mmsi
                ORDER BY (length_m IS NOT NULL AND length_m > 0) DESC, {order_ts}) AS rn
            FROM static_raw WHERE mmsi IS NOT NULL
        ) WHERE rn = 1
    """)
    static_cols = {r[0] for r in con.execute("DESCRIBE static").fetchall()}
    pick = lambda c: f"s.{c}" if c in static_cols else f"NULL AS {c}"  # noqa: E731
    con.execute(f"""
        CREATE TABLE vessels AS
        SELECT m.mmsi,
               {pick('vessel_name')}, {pick('imo')}, {pick('ship_type')},
               {pick('flag_mid')}, s.length_m,
               CASE
                 WHEN s.mmsi IS NULL                          THEN 'unknown_length'
                 WHEN s.length_m IS NULL OR s.length_m <= 0   THEN 'unknown_length'
                 WHEN s.length_m >= {MIN_HULL_LENGTH_M}       THEN 'large_hull'
                 ELSE 'below_min_length'
               END AS hull_class
        FROM (SELECT DISTINCT mmsi FROM pos) m
        LEFT JOIN static s USING (mmsi)
    """)

    # ------------------------------------------------------------------ #
    # 4. Dark-gap detector
    # ------------------------------------------------------------------ #
    con.execute(f"""
        CREATE TABLE gaps AS
        WITH seq AS (
            SELECT mmsi, ts_utc, lat, lon, sog_kn, h3_r7,
                   lead(ts_utc) OVER w AS next_ts,
                   lead(lat)    OVER w AS next_lat,
                   lead(lon)    OVER w AS next_lon,
                   lead(sog_kn) OVER w AS next_sog_kn,
                   lead(h3_r7)  OVER w AS next_h3_r7,
                   lag(ts_utc)  OVER w AS prev_ts
            FROM pos
            WINDOW w AS (PARTITION BY mmsi ORDER BY ts_utc)
        )
        SELECT mmsi,
               ts_utc  AS gap_start_utc,
               next_ts AS gap_end_utc,
               (epoch(next_ts) - epoch(ts_utc)) / 60.0 AS gap_minutes,
               lat AS start_lat, lon AS start_lon, h3_r7 AS start_h3_r7,
               next_lat AS end_lat, next_lon AS end_lon, next_h3_r7 AS end_h3_r7,
               sog_kn AS sog_before_kn, next_sog_kn AS sog_after_kn,
               2 * {EARTH_RADIUS_NM} * asin(sqrt(
                   pow(sin(radians(next_lat - lat) / 2), 2) +
                   cos(radians(lat)) * cos(radians(next_lat)) *
                   pow(sin(radians(next_lon - lon) / 2), 2))) AS distance_nm
        FROM seq WHERE next_ts IS NOT NULL
    """)
    con.execute("""
        ALTER TABLE gaps ADD COLUMN implied_speed_kn DOUBLE;
        UPDATE gaps SET implied_speed_kn =
            CASE WHEN gap_minutes > 0 THEN distance_nm / (gap_minutes / 60.0) END;
    """)

    con.execute(f"""
        CREATE TABLE candidates AS
        WITH ov AS (
            SELECT g.mmsi, g.gap_start_utc,
                   coalesce(sum(greatest(0,
                       epoch(least(g.gap_end_utc, o.end_utc)) -
                       epoch(greatest(g.gap_start_utc, o.start_utc)))) / 60.0, 0) AS outage_overlap_minutes,
                   count(o.outage_id) AS outages_overlapped
            FROM gaps g
            LEFT JOIN outages o
              ON o.start_utc < g.gap_end_utc AND o.end_utc > g.gap_start_utc
            WHERE g.gap_minutes > {GAP_MIN_MINUTES}
            GROUP BY g.mmsi, g.gap_start_utc
        )
        SELECT row_number() OVER (ORDER BY g.mmsi, g.gap_start_utc) AS gap_id,
               g.mmsi, v.vessel_name, v.imo, v.ship_type, v.flag_mid, v.length_m, v.hull_class,
               g.gap_start_utc, g.gap_end_utc,
               round(g.gap_minutes, 1)                              AS gap_minutes,
               round(ov.outage_overlap_minutes, 1)                  AS outage_overlap_minutes,
               round(g.gap_minutes - ov.outage_overlap_minutes, 1)  AS effective_gap_minutes,
               ov.outages_overlapped,
               round(g.distance_nm, 2)       AS distance_nm,
               round(g.implied_speed_kn, 2)  AS implied_speed_kn,
               g.sog_before_kn, g.sog_after_kn,
               g.start_lat, g.start_lon, g.start_h3_r7,
               g.end_lat, g.end_lon, g.end_h3_r7,
               CASE
                 WHEN v.hull_class = 'below_min_length'                 THEN 'reject_small_hull'
                 WHEN coalesce(g.sog_before_kn, 0) < {MIN_SOG_KN}       THEN 'reject_not_underway'
                 WHEN g.gap_minutes - ov.outage_overlap_minutes <= {GAP_MIN_MINUTES}
                                                                        THEN 'reject_collector_outage'
                 ELSE 'flagged'
               END AS decision
        FROM gaps g
        JOIN ov USING (mmsi, gap_start_utc)
        JOIN vessels v USING (mmsi)
        ORDER BY g.gap_minutes DESC
    """)

    # ------------------------------------------------------------------ #
    # 5. Anchorage mask + gap category
    # ------------------------------------------------------------------ #
    con.execute(f"""
        CREATE TABLE events AS
        WITH s AS (
            SELECT pid, string_agg(zone_id, ',' ORDER BY zone_id) AS zones,
                   bool_or(counts_as_drift) AS drift_zone
            FROM ({zone_hits("(SELECT gap_id AS pid, start_lat AS lat, start_lon AS lon FROM candidates)")})
            GROUP BY pid
        ),
        e AS (
            SELECT pid, string_agg(zone_id, ',' ORDER BY zone_id) AS zones,
                   bool_or(counts_as_drift) AS drift_zone
            FROM ({zone_hits("(SELECT gap_id AS pid, end_lat AS lat, end_lon AS lon FROM candidates)")})
            GROUP BY pid
        ),
        tagged AS (
            SELECT c.*,
                   s.zones AS start_zone,
                   e.zones AS end_zone,
                   (s.pid IS NOT NULL OR e.pid IS NOT NULL) AS in_anchorage,
                   coalesce(s.drift_zone OR e.drift_zone, s.drift_zone, e.drift_zone, false) AS in_drift_zone,
                   (c.distance_nm < {DRIFT_MAX_DISTANCE_NM}
                    AND c.implied_speed_kn < {DRIFT_MAX_SPEED_KN}) AS low_motion
            FROM candidates c
            LEFT JOIN s ON s.pid = c.gap_id
            LEFT JOIN e ON e.pid = c.gap_id
        )
        SELECT * EXCLUDE (in_drift_zone, low_motion),
               CASE WHEN in_drift_zone OR low_motion THEN 'anchorage_drift'
                    ELSE 'open_water_candidate' END AS gap_category,
               concat_ws('+', CASE WHEN in_drift_zone THEN 'in_anchorage' END,
                              CASE WHEN low_motion    THEN 'low_motion'   END) AS category_reason
        FROM tagged
    """)

    # ------------------------------------------------------------------ #
    # 6. Outputs + summary
    # ------------------------------------------------------------------ #
    flagged_sql = "SELECT * EXCLUDE (decision) FROM events WHERE decision = 'flagged' ORDER BY effective_gap_minutes DESC"
    pq_out = OUT_DIR / "dark_events_phase1.parquet"
    csv_out = OUT_DIR / "dark_events_phase1.csv"
    con.execute(f"COPY ({flagged_sql}) TO {q(pq_out)} (FORMAT parquet, COMPRESSION zstd)")
    con.execute(f"COPY ({flagged_sql}) TO {q(csv_out)} (FORMAT csv, HEADER true)")

    one = lambda sql: con.execute(sql).fetchone()  # noqa: E731
    n_rows, n_mmsi, t0, t1 = one("SELECT count(*), count(DISTINCT mmsi), min(ts_utc), max(ts_utc) FROM pos")
    hours = (t1 - t0).total_seconds() / 3600 if t0 and t1 else 0
    n_out, out_min = one("SELECT count(*), coalesce(sum(minutes), 0) FROM outages")
    n_hex, top_hex, top_pings, top_mmsi = one(
        "SELECT (SELECT count(*) FROM h3_coverage), h3_r7, pings, unique_mmsi FROM h3_coverage LIMIT 1") or (0, None, 0, 0)
    hull = dict(con.execute("SELECT hull_class, count(*) FROM vessels GROUP BY 1").fetchall())
    n_gaps_all = one("SELECT count(*) FROM gaps")[0]
    decisions = dict(con.execute("SELECT decision, count(*) FROM candidates GROUP BY 1").fetchall())
    n_flagged = decisions.get("flagged", 0)
    categories = dict(con.execute(
        f"SELECT gap_category, count(*) FROM ({flagged_sql}) GROUP BY 1").fetchall())
    zone_pings = con.execute(f"""
        WITH pts AS (SELECT row_number() OVER () AS pid, mmsi, lat, lon FROM pos),
             hits AS ({zone_hits("pts")})
        SELECT z.zone_id, z.zone_type, count(h.pid) AS pings, count(DISTINCT p.mmsi) AS mmsi
        FROM zones z
        LEFT JOIN hits h USING (zone_id)
        LEFT JOIN pts p USING (pid)
        GROUP BY ALL ORDER BY pings DESC, z.zone_id
    """).fetchall()

    print("=" * 72)
    print("DRK VSSL  Phase 1B  dark-gap detector")
    print("=" * 72)
    print(f"Data:      {n_rows:,} pings, {n_mmsi:,} MMSI, {t0:%Y-%m-%d %H:%M} -> {t1:%Y-%m-%d %H:%M} UTC ({hours:.1f} h)")
    print(f"Outages:   {n_out} window(s), {out_min:.1f} min total ({', '.join(OUTAGE_STATES)}, heartbeat > {HEARTBEAT_MISSING_SEC}s)")
    print(f"H3 r7:     {n_hex} cells; busiest {top_hex} ({top_pings} pings, {top_mmsi} MMSI)")
    print(f"Hulls:     large_hull(>= {MIN_HULL_LENGTH_M} m)={hull.get('large_hull', 0)}  "
          f"unknown_length={hull.get('unknown_length', 0)}  below_min_length={hull.get('below_min_length', 0)}")
    print(f"Gaps:      {n_gaps_all:,} consecutive-ping intervals; {sum(decisions.values())} longer than {GAP_MIN_MINUTES} min")
    for k in ("reject_small_hull", "reject_not_underway", "reject_collector_outage"):
        print(f"             {k:<26}{decisions.get(k, 0)}")
    print(f"Mask:      {len(zone_pings)} zone(s) from {ANCHORAGE_MASK.name} (approximate); pings inside:")
    for zone_id, zone_type, pings, mmsi in zone_pings:
        print(f"             {zone_id:<28}{zone_type or '?':<13}{pings:>6} pings {mmsi:>5} MMSI")
    print(f"FLAGGED:   {n_flagged} candidate dark gap(s): "
          f"open_water_candidate={categories.get('open_water_candidate', 0)}  "
          f"anchorage_drift={categories.get('anchorage_drift', 0)}")
    if n_flagged:
        print()
        con.sql(f"""
            SELECT mmsi, coalesce(vessel_name, '?') AS name, hull_class,
                   strftime(gap_start_utc, '%H:%M') AS start_utc, strftime(gap_end_utc, '%H:%M') AS end_utc,
                   effective_gap_minutes AS eff_min, distance_nm AS nm, implied_speed_kn AS kn,
                   sog_before_kn AS sog_before, gap_category AS category,
                   coalesce(start_zone, '-') || ' / ' || coalesce(end_zone, '-') AS zone_start_end
            FROM ({flagged_sql}) ORDER BY gap_category DESC, effective_gap_minutes DESC LIMIT 20
        """).show(max_width=200)
    if hours < 2 * GAP_MIN_MINUTES / 60:
        print(f"\nNote: only {hours:.1f} h of data; few gaps can exceed {GAP_MIN_MINUTES} min yet.")
    print("\nNote: a gap can also mean the vessel left and re-entered the AOI; check end points before calling it dark.")
    print(f"\nWrote {h3_out.relative_to(BASE_DIR) if h3_out.is_relative_to(BASE_DIR) else h3_out}")
    print(f"Wrote {pq_out.relative_to(BASE_DIR) if pq_out.is_relative_to(BASE_DIR) else pq_out}")
    print(f"Wrote {csv_out.relative_to(BASE_DIR) if csv_out.is_relative_to(BASE_DIR) else csv_out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
