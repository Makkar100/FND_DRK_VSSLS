#!/usr/bin/env python3
"""Build an interactive dark-theme HTML map of the Phase 1 results for the Eastern OPL AOI.

Inputs (relative to this script's folder):
    data/ais/parquet/ais_positions/date=*/*.parquet   (latest ping per MMSI)
    data/ais/parquet/ais_static/date=*/*.parquet      (name, ship type, length)
    outputs/h3_coverage_phase1.parquet
    outputs/dark_events_phase1.parquet
    masks/anchorages.geojson

Output:
    outputs/opl_map_phase1.html  (standalone; open in any browser, needs internet for tiles)

Layers (toggle with the control at top right):
    - Base maps: OpenStreetMap dark (CSS-filtered, default), OpenStreetMap light, Esri World Imagery
    - AOI bounding box
    - H3 r7 coverage hexes, shaded by ping count
    - Anchorage / OPL masks (name on hover)
    - Dark gaps: open_water_candidate
    - Dark gaps: anchorage_drift
    - All vessels (last known position), ship icons rotated to heading / course

Usage:
    python3 build_map_phase1.py
"""

from __future__ import annotations

import html
import json
import math
from pathlib import Path

import branca.colormap as cm
import folium
import h3
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
POSITIONS_DIR = BASE_DIR / "data" / "ais" / "parquet" / "ais_positions"
STATIC_DIR = BASE_DIR / "data" / "ais" / "parquet" / "ais_static"
COVERAGE_PATH = BASE_DIR / "outputs" / "h3_coverage_phase1.parquet"
EVENTS_PATH = BASE_DIR / "outputs" / "dark_events_phase1.parquet"
MASKS_PATH = BASE_DIR / "masks" / "anchorages.geojson"
OUTPUT_PATH = BASE_DIR / "outputs" / "opl_map_phase1.html"

# Eastern OPL AOI: [south, west], [north, east]
AOI_SW = (1.2, 104.0)
AOI_NE = (2.6, 105.5)

MIN_HULL_LENGTH_M = 120  # same threshold as detect_gaps_phase1.py

# Dark-theme palette
CATEGORY_STYLE = {
    "open_water_candidate": {"color": "#ff5a5f", "weight": 4, "radius": 7, "label": "open water candidate"},
    "anchorage_drift": {"color": "#60a5fa", "weight": 2.5, "radius": 5, "label": "anchorage drift"},
}
DEFAULT_STYLE = {"color": "#9ca3af", "weight": 2, "radius": 5, "label": "other"}

ZONE_COLORS = {
    "anchorage": "#34d399",
    "harbour": "#94a3b8",
    "opl_waiting": "#c084fc",
}

HEX_COLORS = ["#1e1b4b", "#312e81", "#4338ca", "#6366f1", "#a5b4fc"]

# Vessel size tiers. Hull icons are drawn pointing up, then rotated.
VESSEL_TIERS = {
    "large_hull": {"label": f"{MIN_HULL_LENGTH_M} m+ hull", "color": "#fbbf24", "width": 14, "height": 30, "z": 300},
    "below_min_length": {"label": f"under {MIN_HULL_LENGTH_M} m hull", "color": "#67e8f9", "width": 10, "height": 20, "z": 200},
    "unknown_length": {"label": "unknown_length", "color": "#a5b4cb", "width": 7, "height": 7, "z": 100},
}

# AIS ship type code ranges (ITU-R M.1371).
SHIP_TYPES_EXACT = {
    30: "Fishing", 31: "Towing", 32: "Towing (large)", 33: "Dredging", 34: "Diving ops",
    35: "Military ops", 36: "Sailing", 37: "Pleasure craft", 50: "Pilot", 51: "Search and rescue",
    52: "Tug", 53: "Port tender", 54: "Anti-pollution", 55: "Law enforcement", 58: "Medical transport",
}
SHIP_TYPES_DECADE = {2: "Wing in ground", 4: "High-speed craft", 6: "Passenger", 7: "Cargo", 8: "Tanker", 9: "Other"}

DARK_CSS = """
<style>
  .leaflet-container { background: #0b0f17; }
  /* Dark OSM: filter the base tiles only, never the overlay or marker panes. */
  .leaflet-tile-pane .dark-osm-tiles {
    filter: invert(100%) hue-rotate(180deg) brightness(95%) contrast(90%) saturate(60%);
  }
  .leaflet-control-layers, .leaflet-bar a, .leaflet-control-scale-line {
    background: rgba(17, 24, 39, 0.92) !important; color: #e5e7eb !important;
    border-color: #374151 !important;
  }
  .leaflet-control-layers { border-radius: 8px; box-shadow: 0 2px 12px rgba(0,0,0,0.5); font: 12px system-ui, sans-serif; }
  .leaflet-control-layers-separator { border-top-color: #374151; }
  .leaflet-popup-content-wrapper, .leaflet-popup-tip {
    background: #111827 !important; color: #e5e7eb !important; box-shadow: 0 4px 16px rgba(0,0,0,0.6);
  }
  .leaflet-popup-content-wrapper { border: 1px solid #374151; border-radius: 8px; }
  .leaflet-container a.leaflet-popup-close-button { color: #9ca3af; }
  .leaflet-tooltip {
    background: rgba(17, 24, 39, 0.95); color: #e5e7eb; border: 1px solid #374151;
    box-shadow: 0 2px 8px rgba(0,0,0,0.5); font: 12px system-ui, sans-serif;
  }
  .leaflet-tooltip-top:before { border-top-color: #374151; }
  .leaflet-tooltip-bottom:before { border-bottom-color: #374151; }
  .leaflet-tooltip-left:before { border-left-color: #374151; }
  .leaflet-tooltip-right:before { border-right-color: #374151; }
  .leaflet-control-attribution { background: rgba(17,24,39,0.7) !important; color: #9ca3af; }
  .leaflet-control-attribution a { color: #93c5fd; }
  /* branca colour scale (top right) */
  .legend { background: rgba(17,24,39,0.85); border-radius: 6px; padding: 4px 6px; }
  .legend text, .legend .caption { fill: #e5e7eb !important; color: #e5e7eb; }
  .legend .tick line, .legend .domain { stroke: #6b7280; }
  .info-table th { text-align: left; padding-right: 10px; color: #9ca3af; font-weight: 500; white-space: nowrap; }
  .info-table td { color: #f3f4f6; }
  .info-table { font: 12px system-ui, sans-serif; border-collapse: collapse; }
  .info-table tr + tr th, .info-table tr + tr td { padding-top: 2px; }
  .vessel-icon { background: none; border: none; }
  .vessel-dot { border-radius: 50%; }
</style>
"""


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def is_missing(value) -> bool:
    return value is None or (not isinstance(value, str) and pd.isna(value))


def fmt(value, digits: int = 1, unit: str = "") -> str:
    """Format a number for popups; show 'n/a' for missing values."""
    if is_missing(value):
        return "n/a"
    return f"{value:,.{digits}f}{unit}"


def text(value, empty: str = "-") -> str:
    """Return a display string; missing or blank values become `empty`."""
    if is_missing(value) or str(value).strip() == "":
        return empty
    return str(value)


def info_popup(rows: list[tuple[str, object]], max_width: int = 360) -> folium.Popup:
    body = "".join(
        f"<tr><th>{html.escape(k)}</th><td>{html.escape(str(v))}</td></tr>" for k, v in rows
    )
    return folium.Popup(f"<table class='info-table'>{body}</table>", max_width=max_width)


def ship_type_label(code) -> str:
    if is_missing(code) or int(code) <= 0:
        return "n/a"
    code = int(code)
    name = SHIP_TYPES_EXACT.get(code) or SHIP_TYPES_DECADE.get(code // 10, "Unknown")
    return f"{name} ({code})"


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_latest_vessels() -> pd.DataFrame:
    """Latest ping per MMSI, joined to the best static record for that MMSI."""
    positions = pd.read_parquet(
        POSITIONS_DIR,
        columns=["ts_utc", "mmsi", "lat", "lon", "sog_kn", "cog_deg", "true_heading"],
    )
    positions = positions.dropna(subset=["lat", "lon"])
    latest = (
        positions.sort_values("ts_utc")
        .drop_duplicates("mmsi", keep="last")
        .reset_index(drop=True)
    )

    # Static has several rows per MMSI: keep the newest row that has a length,
    # else the newest row (same rule as detect_gaps_phase1.py).
    static = pd.read_parquet(
        STATIC_DIR, columns=["ts_utc", "mmsi", "vessel_name", "ship_type", "length_m"]
    )
    static["has_length"] = static["length_m"].fillna(0) > 0
    static = (
        static.sort_values(["has_length", "ts_utc"])
        .drop_duplicates("mmsi", keep="last")
        .drop(columns=["ts_utc", "has_length"])
    )

    vessels = latest.merge(static, on="mmsi", how="left")

    # AIS "not available" values: heading 511, course 360. The compactor already
    # nulls them; this guards older files.
    vessels["true_heading"] = vessels["true_heading"].where(vessels["true_heading"].between(0, 359))
    vessels["cog_deg"] = vessels["cog_deg"].where((vessels["cog_deg"] >= 0) & (vessels["cog_deg"] < 360))
    vessels["bearing"] = vessels["true_heading"].fillna(vessels["cog_deg"])
    vessels["bearing_source"] = "n/a"
    vessels.loc[vessels["cog_deg"].notna(), "bearing_source"] = "cog"
    vessels.loc[vessels["true_heading"].notna(), "bearing_source"] = "true_heading"

    length = vessels["length_m"]
    vessels["hull_class"] = "unknown_length"
    vessels.loc[length > 0, "hull_class"] = "below_min_length"
    vessels.loc[length >= MIN_HULL_LENGTH_M, "hull_class"] = "large_hull"
    return vessels


# ---------------------------------------------------------------------------
# Layers
# ---------------------------------------------------------------------------

def add_base_maps(fmap: folium.Map) -> None:
    """Zero-key base maps. The first one added is shown on load."""
    osm_url = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
    osm_attr = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'

    # Normal OSM tiles darkened by a CSS filter (see .dark-osm-tiles in DARK_CSS).
    # The filter sits on this layer's container inside .leaflet-tile-pane only,
    # so vessels, hexes, gaps and the legend keep their real colours.
    folium.TileLayer(
        tiles=osm_url, attr=osm_attr, name="OpenStreetMap (dark)",
        max_zoom=19, class_name="dark-osm-tiles",
    ).add_to(fmap)
    folium.TileLayer(
        tiles=osm_url, attr=osm_attr, name="OpenStreetMap (light)",
        max_zoom=19, show=False,
    ).add_to(fmap)
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr="Tiles &copy; Esri &mdash; Source: Esri, Maxar, Earthstar Geographics, and the GIS User Community",
        name="Esri World Imagery (satellite)",
        max_zoom=19, show=False,
    ).add_to(fmap)


def add_aoi_box(fmap: folium.Map) -> None:
    layer = folium.FeatureGroup(name="AOI bounding box", show=True)
    folium.Rectangle(
        bounds=[AOI_SW, AOI_NE],
        color="#e5e7eb",
        weight=1.5,
        opacity=0.9,
        dash_array="8 6",
        fill=False,
        tooltip=f"Eastern OPL AOI {list(AOI_SW)} to {list(AOI_NE)}",
    ).add_to(layer)
    layer.add_to(fmap)


def add_coverage(fmap: folium.Map, coverage: pd.DataFrame) -> None:
    layer = folium.FeatureGroup(name=f"H3 r7 coverage ({len(coverage)} cells)", show=True)

    # Ping counts are heavily skewed, so colour on a log scale.
    log_pings = coverage["pings"].clip(lower=1).map(math.log10)
    colormap = cm.LinearColormap(
        colors=HEX_COLORS,
        vmin=float(log_pings.min()),
        vmax=float(log_pings.max()),
        caption="H3 r7 ping count (log10)",
    )

    for row, log_value in zip(coverage.itertuples(index=False), log_pings):
        boundary = h3.cell_to_boundary(row.h3_r7)  # list of (lat, lng)
        tooltip = (
            f"<b>{row.h3_r7}</b><br>"
            f"Pings: {row.pings:,}<br>"
            f"Unique MMSIs: {row.unique_mmsi:,}"
        )
        folium.Polygon(
            locations=boundary,
            color="#818cf8",
            weight=0.4,
            opacity=0.35,
            fill=True,
            fill_color=colormap(log_value),
            fill_opacity=0.35,
            tooltip=folium.Tooltip(tooltip, sticky=True),
        ).add_to(layer)

    layer.add_to(fmap)
    colormap.add_to(fmap)


def add_masks(fmap: folium.Map, masks: dict) -> None:
    layer = folium.FeatureGroup(name=f"Anchorage masks ({len(masks['features'])})", show=True)

    def style(feature: dict) -> dict:
        props = feature["properties"]
        color = ZONE_COLORS.get(props.get("zone_type"), "#9ca3af")
        return {
            "color": color,
            "weight": 1.5,
            "opacity": 0.9,
            "fillColor": color,
            "fillOpacity": 0.08,
            # Dashed outline = zone does not count as drift (OPL loitering is a signal).
            "dashArray": None if props.get("counts_as_drift", True) else "5 5",
        }

    def highlight(_feature: dict) -> dict:
        return {"weight": 3, "fillOpacity": 0.2}

    # Labels show on hover only, so they never pile up when zoomed out.
    folium.GeoJson(
        masks,
        style_function=style,
        highlight_function=highlight,
        tooltip=folium.GeoJsonTooltip(
            fields=["zone_id", "name", "zone_type", "counts_as_drift"],
            aliases=["Zone", "Name", "Type", "Counts as drift"],
            sticky=True,
        ),
    ).add_to(layer)
    layer.add_to(fmap)


def gap_popup(row) -> folium.Popup:
    hull = row.hull_class if pd.isna(row.length_m) else f"{row.hull_class} ({row.length_m:.0f} m)"
    return info_popup([
        ("Gap ID", row.gap_id),
        ("MMSI", row.mmsi),
        ("Category", row.gap_category),
        ("Hull length", hull),
        ("Gap duration", fmt(row.effective_gap_minutes, 1, " min") + f" (raw {fmt(row.gap_minutes, 1)} min)"),
        ("Distance", fmt(row.distance_nm, 2, " nm")),
        ("Implied speed", fmt(row.implied_speed_kn, 2, " kn")),
        ("Start / end zone", f"{text(row.start_zone, 'open water')} / {text(row.end_zone, 'open water')}"),
        ("Gap start (UTC)", row.gap_start_utc.strftime("%Y-%m-%d %H:%M")),
        ("Gap end (UTC)", row.gap_end_utc.strftime("%Y-%m-%d %H:%M")),
        ("Reason", text(row.category_reason, "no drift rule matched")),
    ])


def add_gaps(fmap: folium.Map, events: pd.DataFrame) -> None:
    # Draw drift first so open-water candidates sit on top.
    order = ["anchorage_drift", "open_water_candidate"]
    categories = order + sorted(set(events["gap_category"]) - set(order))

    for category in categories:
        subset = events[events["gap_category"] == category]
        if subset.empty:
            continue
        style = CATEGORY_STYLE.get(category, DEFAULT_STYLE)
        layer = folium.FeatureGroup(name=f"Dark gaps: {style['label']} ({len(subset)})", show=True)

        for row in subset.itertuples(index=False):
            start = (row.start_lat, row.start_lon)
            end = (row.end_lat, row.end_lon)
            tooltip = f"MMSI {row.mmsi} | {style['label']} | {fmt(row.effective_gap_minutes, 0, ' min')}"

            folium.PolyLine(
                locations=[start, end],
                color=style["color"],
                weight=style["weight"],
                opacity=0.95,
                dash_array="1 6" if category == "anchorage_drift" else None,
                line_cap="round",
                tooltip=tooltip,
                popup=gap_popup(row),
            ).add_to(layer)
            # Start = hollow circle (went dark); end = filled circle (reappeared).
            for location, fill, label in ((start, "#0b0f17", "Start (went dark)"), (end, style["color"], "End (reappeared)")):
                folium.CircleMarker(
                    location=location,
                    radius=style["radius"],
                    color=style["color"],
                    weight=2,
                    fill=True,
                    fill_color=fill,
                    fill_opacity=1.0,
                    tooltip=f"{label}: {tooltip}",
                    popup=gap_popup(row),
                ).add_to(layer)

        layer.add_to(fmap)


def hull_svg(color: str, width: int, height: int) -> str:
    """Pointed ship-hull outline, bow pointing up (north)."""
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 20 40">'
        f'<path d="M10 1 L18 14 L18 36 Q18 39 15 39 L5 39 Q2 39 2 36 L2 14 Z" '
        f'fill="{color}" fill-opacity="0.95" stroke="#0b0f17" stroke-width="1.5" stroke-linejoin="round"/>'
        f'<rect x="6" y="27" width="8" height="6" rx="1" fill="#0b0f17" fill-opacity="0.45"/>'
        f"</svg>"
    )


def vessel_icon(hull_class: str, bearing) -> tuple[folium.DivIcon, int]:
    tier = VESSEL_TIERS[hull_class]
    color = tier["color"]

    if hull_class == "unknown_length":
        size = tier["width"]
        icon_html = (
            f'<div class="vessel-dot" style="width:{size}px;height:{size}px;background:{color};'
            f'opacity:0.75;box-shadow:0 0 6px 2px {color}66;"></div>'
        )
        return folium.DivIcon(html=icon_html, icon_size=(size, size), icon_anchor=(size / 2, size / 2),
                              class_name="vessel-icon"), tier["z"]

    box = tier["height"]  # square box so rotation stays centred
    if is_missing(bearing):
        # No heading or course: draw a ring instead of a hull that points nowhere.
        ring = tier["width"]
        icon_html = (
            f'<div style="width:{box}px;height:{box}px;display:flex;align-items:center;justify-content:center;">'
            f'<div class="vessel-dot" style="width:{ring}px;height:{ring}px;border:2px solid {color};'
            f'background:{color}33;"></div></div>'
        )
    else:
        icon_html = (
            f'<div style="width:{box}px;height:{box}px;display:flex;align-items:center;justify-content:center;'
            f'transform:rotate({float(bearing):.0f}deg);filter:drop-shadow(0 0 3px {color}88);">'
            f"{hull_svg(color, tier['width'], tier['height'])}</div>"
        )
    return folium.DivIcon(html=icon_html, icon_size=(box, box), icon_anchor=(box / 2, box / 2),
                          class_name="vessel-icon"), tier["z"]


def vessel_popup(row) -> folium.Popup:
    if is_missing(row.length_m) or row.length_m <= 0:
        length = "unknown_length"
    else:
        length = f"{row.length_m:.0f} m"
    heading = "n/a" if is_missing(row.bearing) else f"{row.bearing:.0f}° ({row.bearing_source})"
    return info_popup([
        ("MMSI", row.mmsi),
        ("Name", text(row.vessel_name, "unknown")),
        ("Ship type", ship_type_label(row.ship_type)),
        ("Length", length),
        ("Last seen (UTC)", row.ts_utc.strftime("%Y-%m-%d %H:%M:%S")),
        ("Heading", heading),
        ("Speed (SOG)", fmt(row.sog_kn, 1, " kn")),
        ("Position", f"{row.lat:.5f}, {row.lon:.5f}"),
    ], max_width=300)


def add_vessels(fmap: folium.Map, vessels: pd.DataFrame) -> None:
    layer = folium.FeatureGroup(name=f"All vessels, last known position ({len(vessels)})", show=True)

    # Small dots first, big hulls last, so large ships sit on top.
    draw_order = {"unknown_length": 0, "below_min_length": 1, "large_hull": 2}
    vessels = vessels.sort_values("hull_class", key=lambda s: s.map(draw_order))

    for row in vessels.itertuples(index=False):
        icon, z = vessel_icon(row.hull_class, row.bearing)
        name = text(row.vessel_name, "")
        folium.Marker(
            location=(row.lat, row.lon),
            icon=icon,
            z_index_offset=z,
            tooltip=f"{name} (MMSI {row.mmsi})" if name else f"MMSI {row.mmsi}",
            popup=vessel_popup(row),
        ).add_to(layer)

    layer.add_to(fmap)


def add_legend(fmap: folium.Map, tier_counts: dict[str, int]) -> None:
    def row(swatch: str, label: str) -> str:
        return (
            "<div style='display:flex;align-items:center;gap:8px;margin-top:4px'>"
            f"<span style='width:22px;display:inline-flex;justify-content:center'>{swatch}</span>"
            f"<span>{label}</span></div>"
        )

    gaps = "".join(
        row(f"<span style='width:20px;height:3px;background:{s['color']};border-radius:2px'></span>", s["label"])
        for s in CATEGORY_STYLE.values()
    )

    ships = ""
    for hull_class, tier in VESSEL_TIERS.items():
        if hull_class == "unknown_length":
            swatch = (f"<span style='width:7px;height:7px;border-radius:50%;background:{tier['color']};"
                      f"box-shadow:0 0 6px 2px {tier['color']}66'></span>")
        else:
            swatch = hull_svg(tier["color"], tier["width"] * 3 // 4, tier["height"] * 3 // 4)
        ships += row(swatch, f"{tier['label']} ({tier_counts.get(hull_class, 0)})")

    zones = "".join(
        row(f"<span style='width:14px;height:10px;border:1.5px solid {c};background:{c}22'></span>",
            z.replace("_", " "))
        for z, c in ZONE_COLORS.items()
    )

    heading = "font-size:11px;letter-spacing:0.06em;text-transform:uppercase;color:#9ca3af;margin-top:10px"
    legend = f"""
    <div style="position: fixed; bottom: 28px; left: 12px; z-index: 9999; min-width: 190px;
                background: rgba(17, 24, 39, 0.9); color: #e5e7eb; padding: 10px 12px;
                border: 1px solid #374151; border-radius: 10px;
                box-shadow: 0 4px 16px rgba(0,0,0,0.5); font: 12px system-ui, sans-serif;
                backdrop-filter: blur(4px);">
      <div style="{heading};margin-top:0">Vessels (last position)</div>{ships}
      <div style="color:#9ca3af;margin-top:4px">hull points to heading (else COG)</div>
      <div style="{heading}">Dark gaps</div>{gaps}
      <div style="color:#9ca3af;margin-top:4px">&#9675; went dark &nbsp; &#9679; reappeared</div>
      <div style="{heading}">Masks</div>{zones}
      <div style="color:#9ca3af;margin-top:4px">dashed = OPL zone, not drift</div>
    </div>
    """
    fmap.get_root().html.add_child(folium.Element(legend))


def main() -> None:
    coverage = pd.read_parquet(COVERAGE_PATH)
    events = pd.read_parquet(EVENTS_PATH)
    masks = json.loads(MASKS_PATH.read_text())
    vessels = load_latest_vessels()

    center = ((AOI_SW[0] + AOI_NE[0]) / 2, (AOI_SW[1] + AOI_NE[1]) / 2)
    fmap = folium.Map(location=center, zoom_start=9, tiles=None, control_scale=True)
    # Added to the body so it loads after (and overrides) Leaflet's own CSS.
    fmap.get_root().html.add_child(folium.Element(DARK_CSS))

    add_base_maps(fmap)
    add_coverage(fmap, coverage)
    add_masks(fmap, masks)
    add_aoi_box(fmap)
    add_gaps(fmap, events)
    add_vessels(fmap, vessels)

    tier_counts = vessels["hull_class"].value_counts().to_dict()
    add_legend(fmap, tier_counts)

    folium.LayerControl(collapsed=False).add_to(fmap)
    fmap.fit_bounds([AOI_SW, AOI_NE])

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fmap.save(str(OUTPUT_PATH))

    sources = vessels["bearing_source"].value_counts().to_dict()
    print(f"Coverage cells: {len(coverage)}")
    print(f"Anchorage masks: {len(masks['features'])}")
    print(f"Dark gaps: {len(events)} {events['gap_category'].value_counts().to_dict()}")
    print(f"Vessels: {len(vessels)} {tier_counts}")
    print(f"Icon direction source: {sources}")
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
