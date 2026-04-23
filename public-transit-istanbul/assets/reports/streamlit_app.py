import io
from pathlib import Path

import altair as alt
import numpy as np
import pandas as pd
import pydeck as pdk
import requests
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(page_title="Istanbul Public Transit", layout="wide")

PROJECT_ID = "bruin-playground-arsalan"
base_path = Path(__file__).parent

# IBM Design Language — Color Blind Safe palette
# https://www.ibm.com/design/language/color/
IBM_BLUE = "#648FFF"
IBM_PURPLE = "#785EF0"
IBM_MAGENTA = "#DC267F"
IBM_ORANGE = "#FE6100"
IBM_YELLOW = "#FFB000"
PALETTE = [IBM_BLUE, IBM_PURPLE, IBM_MAGENTA, IBM_ORANGE, IBM_YELLOW]
GREY = "#878D96"

MODE_NAMES = {"OTOYOL": "Bus & Road", "RAYLI": "Rail", "DENİZ": "Ferry"}
MODE_COLORS = {"Bus & Road": IBM_BLUE, "Rail": IBM_MAGENTA, "Ferry": IBM_YELLOW}


@st.cache_resource
def get_client():
    credentials = service_account.Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)


def run_query(filename: str) -> pd.DataFrame:
    sql = (base_path / filename).read_text()
    return get_client().query(sql).to_dataframe()


def run_raw(sql: str) -> pd.DataFrame:
    return get_client().query(sql).to_dataframe()


@st.cache_data(ttl=86400)
def load_district_geojson():
    """Load Istanbul district boundary GeoJSON (39 ilce polygons)."""
    url = (
        "https://raw.githubusercontent.com/ozanyerli/"
        "istanbul-districts-geojson/main/istanbul-districts.json"
    )
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


@st.cache_data(ttl=86400)
def load_district_population():
    """Load Istanbul district population from IBB Open Data (TUIK census)."""
    url = (
        "https://data.ibb.gov.tr/en/dataset/"
        "6646992d-e1cf-4d5b-bb79-3f92dcc3fc38/resource/"
        "da5a16eb-50a2-4264-88d1-92e4a859a8f7/download/nufus-bilgileri.xlsx"
    )
    try:
        resp = requests.get(url, timeout=30, verify=False)
        resp.raise_for_status()
        df = pd.read_excel(io.BytesIO(resp.content))
        # Sum all age bracket columns to get total population per district/year
        age_cols = [c for c in df.columns if c not in ("Yıl", "İlçe", "ilce_kodu")]
        df["total_population"] = df[age_cols].sum(axis=1)
        latest_year = df["Yıl"].max()
        latest = df[df["Yıl"] == latest_year][["İlçe", "total_population"]].copy()
        latest.columns = ["district", "population"]
        return latest
    except Exception:
        return None


def nearest_neighbor_order(df: pd.DataFrame) -> pd.DataFrame:
    """Order stations along a line using greedy nearest-neighbor chain.

    Starts from the station furthest from the centroid (likely an endpoint).
    """
    coords = df[["longitude", "latitude"]].values
    n = len(coords)
    if n <= 2:
        return df

    centroid = coords.mean(axis=0)
    dists_to_center = ((coords - centroid) ** 2).sum(axis=1)
    start = int(dists_to_center.argmax())

    visited = [start]
    remaining = set(range(n)) - {start}

    while remaining:
        last = coords[visited[-1]]
        nearest = min(
            remaining,
            key=lambda i: (coords[i][0] - last[0]) ** 2
            + (coords[i][1] - last[1]) ** 2,
        )
        visited.append(nearest)
        remaining.remove(nearest)

    return df.iloc[visited]


# Manual station orders for lines where nearest-neighbor fails due to
# non-linear geometry (V-shapes, sharp turns, spurs).  Station names must
# match the IBB GeoJSON station_name values.  Stations in the data but
# missing from the list are appended at the end via nearest-neighbor.
_MANUAL_STATION_ORDER: dict[str, list[str]] = {
    # M11 forms an inverted-V: Kağıthane → Airport → Halkalı
    # Airport station order verified by coordinate distance from İhsaniye:
    #   -2 (3.7 km) → -1 (5.8 km) → -3 (8.2 km, closest to Arnavutköy side)
    "M11": [
        "Kağıthane",
        "Hasdal",
        "Kemerburgaz",
        "Göktürk",
        "İhsaniye",
        "Yeni Havalimanı - 2",
        "Yeni Havalimanı - 1",
        "Yeni Havalimanı - 3",
        "Arnavutköy -2",
        "Arnavutköy - 1",
        "Fenertepe",
        "Kayaşehir Merkez",
        "Olimpiyat",
        "Halkalı",
    ],
}


def _apply_manual_order(df: pd.DataFrame, order: list[str]) -> pd.DataFrame:
    """Reorder df rows to match a manual station sequence.

    Stations present in *order* appear first (in that sequence); any stations
    in df but missing from *order* are appended at the end sorted by nearest-
    neighbor so new/renamed stations still render.
    """
    name_to_idx = {name: i for i, name in enumerate(order)}
    known = df[df["station_name"].isin(name_to_idx)]
    unknown = df[~df["station_name"].isin(name_to_idx)]

    # Sort known stations by their manual index
    known = known.copy()
    known["_order"] = known["station_name"].map(name_to_idx)
    known = known.sort_values("_order").drop(columns=["_order"])

    if unknown.empty:
        return known
    # Append unknowns via nearest-neighbor from the last known station
    return pd.concat([known, nearest_neighbor_order(unknown)], ignore_index=True)


# Line name overrides for GeoJSON entries that don't follow the standard
# naming pattern (e.g. missing line code prefix, or branches that should
# be kept separate from the trunk line).
_LINE_NAME_OVERRIDES: dict[str, str] = {
    # Alibeyköy Cep Otogarı is T5's terminus but listed under a different name
    "Eminönü - Eyüp - Alibeyköy Tramvay Hattı": "T5",
    # M4 Tuzla extension branches at Tavşantepe — keep separate to avoid Y-fork
    "M4 Tavşantepe - Tuzla Metro Hattı Uzatması": "M4B",
}


def _extract_line_code(line_name: str) -> str:
    """Extract the base line code (e.g. 'M7', 'M1B', 'T1') from a full line name.

    Lines like 'M7 (U3) Mahmutbey - ...' and 'M7 Yıldız - ...' both map to 'M7',
    so their stations get merged into a single connected path.
    """
    import re

    if line_name in _LINE_NAME_OVERRIDES:
        return _LINE_NAME_OVERRIDES[line_name]
    m = re.match(r"^(M\d+[A-Z]?|T\d+|F\d+|TF\d+)", line_name)
    return m.group(1) if m else line_name


def build_line_paths(geo_df: pd.DataFrame) -> list[dict]:
    """Build ordered coordinate paths for each rail line.

    Merges segments that share the same line code (e.g. M7 existing +
    M7 U2/U3/U4 construction) into a single connected path.
    """
    # Group by extracted line code so extensions connect to the main line
    geo_df = geo_df.copy()
    geo_df["_line_code"] = geo_df["line_name"].apply(_extract_line_code)

    paths = []
    for line_code, group in geo_df.groupby("_line_code"):
        # Order ALL stations on the line together for correct connectivity
        all_stations = group.drop_duplicates(subset=["station_name"])
        if len(all_stations) < 2:
            continue
        if line_code in _MANUAL_STATION_ORDER:
            ordered = _apply_manual_order(all_stations, _MANUAL_STATION_ORDER[line_code])
        else:
            ordered = nearest_neighbor_order(all_stations)
        ordered_list = list(ordered.iterrows())

        # Split into contiguous segments by phase so existing = black,
        # construction = orange, even within the same line
        segment: list[dict] = []
        prev_phase = None
        for _, r in ordered_list:
            is_existing = r["project_phase"] == "Mevcut Hattaki İstasyon"
            if prev_phase is not None and is_existing != prev_phase:
                # Phase changed — close current segment (overlap last point)
                if len(segment) >= 2:
                    paths.append(
                        _make_path_record(
                            segment, line_code, all_stations, prev_phase
                        )
                    )
                # Start new segment with overlap point for continuity
                segment = [segment[-1]] if segment else []
            segment.append(
                {"longitude": r["longitude"], "latitude": r["latitude"]}
            )
            prev_phase = is_existing

        # Flush final segment
        if len(segment) >= 2 and prev_phase is not None:
            paths.append(
                _make_path_record(
                    segment, line_code, all_stations, prev_phase
                )
            )

    return paths


def _make_path_record(
    segment: list[dict], line_code: str, all_stations, is_existing: bool
) -> dict:
    path = [[s["longitude"], s["latitude"]] for s in segment]
    status = "existing" if is_existing else "under construction"
    return {
        "path": path,
        "name": line_code,
        "detail": (
            f"{all_stations.iloc[0]['line_type']} ({status}) "
            f"| {len(segment)} stations"
        ),
        "color": [30, 30, 30, 180] if is_existing else [254, 97, 0, 140],
        "width": 5 if is_existing else 4,
    }


def _normalize_turkish(name: str) -> str:
    """Convert Turkish name to uppercase ASCII for cross-dataset matching."""
    tr_map = str.maketrans("çğıöşüÇĞİÖŞÜâî", "cgiosuCGIOSUai")
    return name.translate(tr_map).upper()


def _polygon_area_km2(coords: list) -> float:
    """Approximate area of a polygon ring in km2 (Shoelace on WGS84)."""
    import math

    km_per_deg_lat = 111.32
    km_per_deg_lon = 111.32 * math.cos(math.radians(41.0))  # Istanbul lat
    area = 0.0
    n = len(coords)
    for i in range(n):
        j = (i + 1) % n
        x1, y1 = coords[i][0] * km_per_deg_lon, coords[i][1] * km_per_deg_lat
        x2, y2 = coords[j][0] * km_per_deg_lon, coords[j][1] * km_per_deg_lat
        area += x1 * y2 - x2 * y1
    return abs(area) / 2.0


def _compute_district_areas(geojson: dict) -> dict[str, float]:
    """Compute area in km2 for each district in the GeoJSON."""
    areas: dict[str, float] = {}
    for feature in geojson["features"]:
        name = feature["properties"]["name"]
        geom = feature["geometry"]
        coord_sets = (
            geom["coordinates"]
            if geom["type"] == "MultiPolygon"
            else [geom["coordinates"]]
        )
        areas[name] = sum(_polygon_area_km2(poly[0]) for poly in coord_sets)
    return areas


def build_choropleth_polygons(
    geojson: dict,
    value_lookup: dict[str, float],
    area_lookup: dict[str, float],
    base_rgb: tuple[int, int, int],
    label_fn,
) -> list[dict]:
    """Build PolygonLayer data shaded by value/km2 density.

    Args:
        geojson: District boundary GeoJSON.
        value_lookup: district name -> raw value (population, passages, etc.)
        area_lookup: district name -> area in km2.
        base_rgb: (R, G, B) base color; intensity/opacity scale with density.
        label_fn: callable(name, value, area_km2, density) -> detail string.
    """
    densities = {}
    for name, area in area_lookup.items():
        val = value_lookup.get(name, 0)
        densities[name] = val / area if area > 0 else 0

    max_density = max(densities.values(), default=1)

    polygons = []
    for feature in geojson["features"]:
        name = feature["properties"]["name"]
        density = densities.get(name, 0)
        val = value_lookup.get(name, 0)
        area = area_lookup.get(name, 0)
        ratio = density / max_density if max_density else 0
        opacity = int(20 + ratio * 150)
        r = int(base_rgb[0] * 0.3 + base_rgb[0] * 0.7 * ratio)
        g = int(base_rgb[1] * 0.3 + base_rgb[1] * 0.7 * ratio)
        b = int(base_rgb[2] * 0.3 + base_rgb[2] * 0.7 * ratio)
        geom = feature["geometry"]
        coord_sets = (
            geom["coordinates"]
            if geom["type"] == "MultiPolygon"
            else [geom["coordinates"]]
        )
        for poly_coords in coord_sets:
            polygons.append(
                {
                    "polygon": poly_coords[0],
                    "name": name,
                    "detail": label_fn(name, val, area, density),
                    "fill_color": [r, g, b, opacity],
                }
            )
    return polygons


def map_legend_html(items: list[tuple[str, str]]) -> str:
    """Build an HTML legend for PyDeck maps.
    items: list of (color_hex, label) tuples.
    """
    swatches = "".join(
        f'<span style="display:inline-flex;align-items:center;margin-right:18px;">'
        f'<span style="width:14px;height:14px;border-radius:3px;background:{color};'
        f'display:inline-block;margin-right:6px;border:1px solid #555;"></span>'
        f'<span style="color:#e0e0e0;font-size:13px;">{label}</span></span>'
        for color, label in items
    )
    return (
        f'<div style="background:#1a1a2e;padding:10px 14px;border-radius:6px;'
        f'margin-top:-8px;margin-bottom:12px;">{swatches}</div>'
    )


# --- Header ---
st.title("Istanbul Public Transit Dashboard")

st.markdown(
    "Istanbulkart tap records across Istanbul's bus, metro, ferry, and Marmaray "
    "networks from January 2020 through October 2024. The underlying data includes "
    "718 million rows of hourly transport data, 1.9 million station-level rail records "
    "across 346 stations on 23 lines, and ferry ridership across 73 piers. "
    "All data sourced from the "
    "[Istanbul Metropolitan Municipality (IBB) Open Data Portal](https://data.ibb.gov.tr/en/)."
)

# --- Load data ---
rail_yearly = run_query("rail_ridership_by_year.sql")
monthly = run_query("monthly_ridership.sql")
ferry_yearly = run_query("ferry_trends.sql")

# --- KPI Metrics ---
latest_rail = rail_yearly[rail_yearly["transaction_year"] == 2024].iloc[0]
prev_rail = rail_yearly[rail_yearly["transaction_year"] == 2023].iloc[0]
latest_ferry = ferry_yearly[ferry_yearly["year"] == 2024].iloc[0]

total_passages = monthly["monthly_passages"].sum()

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric(
        "Total Passages (2020-2024)",
        f"{total_passages / 1e9:.1f}B",
    )
with col2:
    st.metric(
        "Rail Passages (2024)",
        f"{latest_rail['total_passages'] / 1e9:.2f}B",
        delta=f"{(latest_rail['total_passages'] - prev_rail['total_passages']) / prev_rail['total_passages'] * 100:+.1f}%",
    )
with col3:
    st.metric(
        "Active Rail Stations (2024)",
        int(latest_rail["active_stations"]),
        delta=f"+{int(latest_rail['active_stations'] - prev_rail['active_stations'])} new",
    )
with col4:
    st.metric(
        "Ferry Journeys (2024)",
        f"{latest_ferry['total_journeys'] / 1e6:.0f}M",
    )

st.divider()

# ===================================================================
# SECTION 1: Monthly ridership by transit mode (2020-2024)
# ===================================================================
st.subheader("Monthly Ridership by Transit Mode, Jan 2020 - Oct 2024")

monthly["mode"] = monthly["road_type"].map(MODE_NAMES)
monthly["month_date"] = pd.to_datetime(monthly["month_date"])
monthly["passages_millions"] = monthly["monthly_passages"] / 1e6

mode_order = ["Bus & Road", "Rail", "Ferry"]
selection = alt.selection_point(fields=["mode"], bind="legend")

st.markdown(
    "All three modes fell sharply in April 2020 during COVID lockdowns. "
    "Bus and rail ridership recovered to pre-pandemic levels by mid-2022, "
    "while ferry ridership plateaued below its January 2020 baseline. "
    "August-October 2024 data is incomplete on the IBB portal (significantly "
    "fewer records per day), causing the apparent drop at the end of the series. "
    "Click a legend entry to isolate a single mode."
)

line_chart = (
    alt.Chart(monthly)
    .mark_line(strokeWidth=2)
    .encode(
        x=alt.X("month_date:T", title="Month"),
        y=alt.Y(
            "passages_millions:Q",
            title="Monthly Passages (Millions)",
            scale=alt.Scale(zero=True),
        ),
        color=alt.Color(
            "mode:N",
            title="Transit Mode",
            scale=alt.Scale(
                domain=mode_order,
                range=[MODE_COLORS[m] for m in mode_order],
            ),
            sort=mode_order,
        ),
        strokeDash=alt.StrokeDash(
            "mode:N",
            scale=alt.Scale(
                domain=mode_order,
                range=[[1, 0], [6, 4], [2, 2]],
            ),
            legend=None,
        ),
        opacity=alt.condition(selection, alt.value(1), alt.value(0.15)),
        tooltip=[
            alt.Tooltip("mode:N", title="Mode"),
            alt.Tooltip("month_date:T", title="Month", format="%b %Y"),
            alt.Tooltip("passages_millions:Q", title="Passages (Millions)", format=",.1f"),
        ],
    )
    .add_params(selection)
    .properties(height=380)
)

# COVID lockdown reference line
covid_rule = (
    alt.Chart(pd.DataFrame({"date": [pd.Timestamp("2020-04-01")]}))
    .mark_rule(color=GREY, strokeDash=[4, 4])
    .encode(x="date:T")
)
covid_label = (
    alt.Chart(
        pd.DataFrame({"date": [pd.Timestamp("2020-04-01")], "label": ["COVID Lockdown"]})
    )
    .mark_text(align="left", dx=5, dy=-170, fontSize=11, color=GREY)
    .encode(x="date:T", text="label:N")
)

# Incomplete data annotation (Aug-Oct 2024)
incomplete_rule = (
    alt.Chart(pd.DataFrame({"date": [pd.Timestamp("2024-08-01")]}))
    .mark_rule(color=IBM_ORANGE, strokeDash=[4, 4])
    .encode(x="date:T")
)
incomplete_label = (
    alt.Chart(
        pd.DataFrame({"date": [pd.Timestamp("2024-08-01")], "label": ["Incomplete data"]})
    )
    .mark_text(align="left", dx=5, dy=-170, fontSize=11, color=IBM_ORANGE)
    .encode(x="date:T", text="label:N")
)

st.altair_chart(
    line_chart + covid_rule + covid_label + incomplete_rule + incomplete_label,
    use_container_width=True,
)

# Compute COVID drop
pre_covid = (
    monthly[monthly["month_date"] < pd.Timestamp("2020-03-01")]
    .groupby("mode")["monthly_passages"]
    .mean()
)
april_2020 = monthly[
    monthly["month_date"].dt.to_period("M") == "2020-04"
].set_index("mode")["monthly_passages"]
crash_pct = ((april_2020 / pre_covid - 1) * 100).round(1)

st.markdown(
    f"> April 2020 drop vs. Jan-Feb 2020 average: Bus **{crash_pct.get('Bus & Road', -81):.0f}%**, "
    f"Rail **{crash_pct.get('Rail', -87):.0f}%**, Ferry **{crash_pct.get('Ferry', -88):.0f}%**."
)

st.caption(
    "**Source:** IBB Hourly Public Transport Dataset — Istanbulkart tap-in counts, "
    "60 monthly CSVs (Jan 2020 - Oct 2024).  \n"
    "**Tools:** Altair line chart with interactive legend selection; stroke dash patterns "
    "differentiate modes independently of color.  \n"
    "**Methodology:** Monthly sum of number_of_passage across all fare types and districts, "
    "grouped by road_type (OTOYOL = Bus & Road, RAYLI = Rail, DENİZ = Ferry). "
    "Duplicate extraction batches removed via ROW_NUMBER deduplication in staging.  \n"
    "**Limitations:** August-October 2024 data is incomplete on the IBB portal "
    "(~80% fewer records per day than normal months); October cuts off at the 18th. "
    "A single Istanbulkart tap may represent a transfer, not a unique trip."
)

st.divider()

# ===================================================================
# SECTION 2: Hourly ridership pattern by day of week
# ===================================================================
st.subheader("Average Hourly Ridership by Day of Week and Transit Mode")

hourly = run_query("hourly_heatmap.sql")
hourly["mode"] = hourly["road_type"].map(MODE_NAMES)

st.markdown(
    "Rail ridership peaks at 18:00 on weekdays, while bus ridership peaks earlier at 07:00. "
    "Ferry shows a late-afternoon peak and relatively stronger weekend ridership at 17:00 "
    "compared to weekday levels, consistent with leisure and tourism usage."
)

# Normalize within each mode to show relative patterns
hourly["normalized"] = hourly.groupby("mode")["avg_passages"].transform(
    lambda x: x / x.max()
)

day_order = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]

col_left, col_mid, col_right = st.columns(3)
for col, mode_name in zip(
    [col_left, col_mid, col_right], ["Rail", "Bus & Road", "Ferry"]
):
    mode_data = hourly[hourly["mode"] == mode_name].copy()
    with col:
        heat = (
            alt.Chart(mode_data)
            .mark_rect(cornerRadius=2)
            .encode(
                x=alt.X(
                    "transition_hour:O",
                    title="Hour",
                    axis=alt.Axis(labelAngle=0),
                ),
                y=alt.Y("day_name:N", sort=day_order, title=None),
                color=alt.Color(
                    "normalized:Q",
                    title="Relative Intensity",
                    scale=alt.Scale(scheme="blues"),
                    legend=None,
                ),
                tooltip=[
                    alt.Tooltip("day_name:N", title="Day"),
                    alt.Tooltip("transition_hour:O", title="Hour"),
                    alt.Tooltip(
                        "avg_passages:Q", title="Avg Passages", format=",.0f"
                    ),
                    alt.Tooltip(
                        "normalized:Q", title="Relative Intensity", format=".2f"
                    ),
                ],
            )
            .properties(height=220, title=mode_name)
        )
        st.altair_chart(heat, use_container_width=True)

st.caption(
    "**Source:** IBB Hourly Public Transport Dataset, Jan 2020 - Oct 2024.  \n"
    "**Tools:** Altair heatmap (mark_rect) with sequential blue color scale.  \n"
    "**Methodology:** Average of number_of_passage per hour-day combination, "
    "normalized 0-1 within each mode (1.0 = that mode's busiest hour-day cell).  \n"
    "**Limitations:** Averages include COVID-affected months (2020-2021) which suppress "
    "absolute levels. Color scale is per-mode, so intensities are not comparable across modes."
)

st.divider()

# --- Pre-load data used by the ridership heatmap (Section 5) ---
growth_df = run_query("station_growth_map.sql")

# ===================================================================
# SECTION 3: Rail Network Map
# ===================================================================
st.subheader("Rail Network Map")

geo_df = run_query("geo_stations.sql")

# --- Apply station phase corrections ---
# The IBB GeoJSON snapshot (circa early 2023) predates several station openings
# between 2022-2025. Corrections below based on IBB press releases and Wikipedia.
_PHASE_EXISTING = "Mevcut Hattaki İstasyon"
_PHASE_CONSTRUCTION = "İnşaat Aşamasında"

_STATION_NOW_OPEN = {
    "Aşiyan", "Rumeli Hisarüstü",  # F4 cable car, opened Oct 2022
    "Bakırköy İDO/Sahil", "Özgürlük Meydanı", "İncirli",  # M3 ext, Mar 2024
    "Haznedar", "İlkyuva", "Molla Gürani",  # M3 ext, Mar 2024
    "Yıldıztepe",  # M3 ext, Oct 2025
    "Meclis", "Sarıgazi", "Sancaktepe", "Samandıra Merkez",  # M5 ext, Mar 2024
    "MASKO",  # M9, opened May 2021
}
_LINES_FULLY_OPEN = {"F4", "M9"}

_line_codes = geo_df["line_name"].apply(_extract_line_code)
_mask_station = (
    (geo_df["project_phase"] == _PHASE_CONSTRUCTION)
    & (geo_df["station_name"].isin(_STATION_NOW_OPEN))
)
_mask_line = (
    (geo_df["project_phase"] == _PHASE_CONSTRUCTION)
    & (_line_codes.isin(_LINES_FULLY_OPEN))
)
_n_corrected = int((_mask_station | _mask_line).sum())
geo_df.loc[_mask_station | _mask_line, "project_phase"] = _PHASE_EXISTING

existing = geo_df[geo_df["project_phase"] == _PHASE_EXISTING].copy()
construction = geo_df[geo_df["project_phase"] == _PHASE_CONSTRUCTION].copy()
existing_count = len(existing)
construction_count = len(construction)

district_geojson = load_district_geojson()
pop_df = load_district_population()
district_areas = (
    _compute_district_areas(district_geojson) if district_geojson else {}
)
line_paths = build_line_paths(geo_df)
district_df = run_query("district_ridership.sql")

existing["name"] = existing["station_name"]
existing["detail"] = existing.apply(
    lambda r: f"{r['line_name']} | {r['line_type']} | Existing", axis=1
)
construction["name"] = construction["station_name"]
construction["detail"] = construction.apply(
    lambda r: f"{r['line_name']} | {r['line_type']} | Under construction", axis=1
)

view_network = pdk.ViewState(latitude=41.02, longitude=29.0, zoom=10.5, pitch=0)
light_tooltip = {
    "html": "<b>{name}</b><br>{detail}",
    "style": {
        "backgroundColor": "#ffffff",
        "color": "#1a1a2e",
        "fontSize": "13px",
        "padding": "8px 12px",
        "border": "1px solid #ccc",
    },
}
light_basemap = "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"


def _make_station_layers():
    """Build the shared station scatter + path layers."""
    lyrs = []
    if line_paths:
        lyrs.append(
            pdk.Layer(
                "PathLayer",
                data=line_paths,
                get_path="path",
                get_color="color",
                get_width="width",
                width_min_pixels=2,
                pickable=True,
            )
        )
    lyrs.append(
        pdk.Layer(
            "ScatterplotLayer",
            data=existing,
            get_position=["longitude", "latitude"],
            get_radius=125,
            get_fill_color=[30, 30, 30, 230],
            pickable=True,
            auto_highlight=True,
        )
    )
    lyrs.append(
        pdk.Layer(
            "ScatterplotLayer",
            data=construction,
            get_position=["longitude", "latitude"],
            get_radius=125,
            get_fill_color=[254, 97, 0, 220],
            pickable=True,
            auto_highlight=True,
            stroked=True,
            get_line_color=[80, 80, 80, 140],
            line_width_min_pixels=1,
        )
    )
    return lyrs


def _light_legend(items):
    """Build a light-background legend HTML bar."""
    swatches = "".join(
        f'<span style="display:inline-flex;align-items:center;margin-right:18px;">'
        f'<span style="width:14px;height:14px;border-radius:3px;background:{c};'
        f'display:inline-block;margin-right:6px;border:1px solid #ccc;"></span>'
        f'<span style="color:#333;font-size:13px;">{lbl}</span></span>'
        for c, lbl in items
    )
    return (
        f'<div style="background:#f5f5f5;padding:10px 14px;border-radius:6px;'
        f'margin-top:-8px;margin-bottom:12px;">{swatches}</div>'
    )


_correction_note = (
    f" {_n_corrected} stations were reclassified from construction to existing "
    "based on IBB announcements and Wikipedia (see Methodology)."
    if _n_corrected > 0
    else ""
)
st.markdown(
    f"Istanbul has **{existing_count}** operational rail stations and "
    f"**{construction_count}** stations under construction across 35 lines."
    f"{_correction_note} "
    "The left map shades districts by population density (2024 TUIK census); "
    "the right map shades by rail Istanbulkart passages (2023). "
    "Ridership counts reflect tap-ins at the station, not the rider's true origin "
    "— transfer hubs like Yenikap\u0131 appear inflated because riders entering "
    "via an interchange may have started their trip in a different district. "
    "Comparing both maps reveals where densely populated districts lack "
    "proportional rail service."
)

# ---- Side-by-side maps ----
col_pop, col_ride = st.columns(2)

# Map A: Population density
pop_layers = []
if district_geojson is not None and pop_df is not None:
    pop_lookup = dict(zip(pop_df["district"], pop_df["population"]))
    pop_polys = build_choropleth_polygons(
        district_geojson,
        pop_lookup,
        district_areas,
        base_rgb=(220, 30, 30),
        label_fn=lambda name, val, area, density: (
            f"Pop: {val:,.0f} | Area: {area:.0f} km\u00b2 | "
            f"Density: {density:,.0f}/km\u00b2"
        ),
    )
    pop_layers.append(
        pdk.Layer(
            "PolygonLayer",
            data=pop_polys,
            get_polygon="polygon",
            get_fill_color="fill_color",
            get_line_color=[100, 100, 100, 60],
            line_width_min_pixels=1,
            stroked=True,
            filled=True,
            pickable=True,
            auto_highlight=True,
        )
    )
pop_layers.extend(_make_station_layers())

with col_pop:
    st.markdown("**Population Density by District**")
    st.pydeck_chart(
        pdk.Deck(
            layers=pop_layers,
            initial_view_state=view_network,
            tooltip=light_tooltip,
            map_style=light_basemap,
        ),
        height=500,
    )

# Map B: Rail ridership
ridership_layers = []
if district_geojson is not None and len(district_df) > 0:
    ridership_by_town = dict(
        zip(district_df["town"], district_df["rail_passages"])
    )
    geo_name_to_ridership = {}
    for feature in district_geojson["features"]:
        geo_name = feature["properties"]["name"]
        normalized = _normalize_turkish(geo_name)
        if normalized in ridership_by_town:
            geo_name_to_ridership[geo_name] = ridership_by_town[normalized]

    unit_areas = {name: 1.0 for name in district_areas}
    ridership_polys = build_choropleth_polygons(
        district_geojson,
        geo_name_to_ridership,
        unit_areas,
        base_rgb=(100, 143, 255),
        label_fn=lambda name, val, area, density: (
            f"Rail passages (2023): {val:,.0f}"
        ),
    )
    ridership_layers.append(
        pdk.Layer(
            "PolygonLayer",
            data=ridership_polys,
            get_polygon="polygon",
            get_fill_color="fill_color",
            get_line_color=[100, 100, 100, 60],
            line_width_min_pixels=1,
            stroked=True,
            filled=True,
            pickable=True,
            auto_highlight=True,
        )
    )
ridership_layers.extend(_make_station_layers())

with col_ride:
    st.markdown("**Rail Ridership by District**")
    st.pydeck_chart(
        pdk.Deck(
            layers=ridership_layers,
            initial_view_state=view_network,
            tooltip=light_tooltip,
            map_style=light_basemap,
        ),
        height=500,
    )

# Shared legend
st.markdown(
    _light_legend([
        ("#1E1E1E", f"Existing stations ({existing_count})"),
        (IBM_ORANGE, f"Under construction ({construction_count})"),
        ("#CC2222", "Population density (red)"),
        ("#4060CC", "Rail ridership (blue)"),
    ]),
    unsafe_allow_html=True,
)

st.markdown("**District ridership and rail coverage (2023):**")

district_display = district_df[
    ["town", "total_passages", "rail_passages", "bus_passages", "ferry_passages", "rail_share_pct"]
].copy()
district_display.columns = [
    "District",
    "Total Passages",
    "Rail",
    "Bus",
    "Ferry",
    "Rail Share %",
]
district_display = district_display.sort_values(
    "Total Passages", ascending=False
).head(20)
st.dataframe(district_display, hide_index=True, use_container_width=True)

no_rail = district_df[
    (district_df["rail_share_pct"] == 0) | (district_df["rail_share_pct"].isna())
]
no_rail_list = no_rail.sort_values("total_passages", ascending=False).head(8)

if not no_rail_list.empty:
    districts_text = ", ".join(
        f"**{r['town']}** ({r['total_passages'] / 1e6:.1f}M trips)"
        for _, r in no_rail_list.iterrows()
    )
    st.markdown(
        f"> Districts with zero rail access: {districts_text}. "
        "These districts rely entirely on bus and road transit."
    )

st.caption(
    "**Source:** IBB Rail Station GeoJSON (June 2025); district boundaries from "
    "ozanyerli/istanbul-districts-geojson (OSM-derived); population from IBB/TUIK "
    "N\u00fcfus Bilgileri (2024 census); ridership from IBB Hourly Public Transport "
    "Dataset (2023).  \n"
    "**Tools:** PyDeck PolygonLayer (district choropleth), PathLayer (rail lines), "
    "ScatterplotLayer (stations) on CartoDB Positron basemap.  \n"
    "**Methodology:** Population map shaded by people/km\u00b2; ridership map shaded by "
    "total rail Istanbulkart passages per district (raw totals, not density-normalized). "
    "Area computed via Shoelace formula at Istanbul's latitude. "
    "Rail lines reconstructed using nearest-neighbor geographic ordering; "
    "lines with non-linear geometry (M11) use manually defined station sequences. "
    f"{_n_corrected} station phases corrected from 'construction' to 'existing' "
    "(F4 Oct 2022, M3 Mar 2024/Oct 2025, M5 Mar 2024, M9 2021-2024).  \n"
    "**Limitations:** Ridership map shows rail passages only — bus excluded because "
    "50-90% of tap records lack a station identifier, making district assignment "
    "unreliable. Ferry excluded as too small to register on the same scale. "
    "Line paths are approximate straight segments, not actual rail alignments. "
    "Population is residential (registered address), not daytime population."
)

st.divider()

# ===================================================================
# SECTION 4: Ridership density heatmap
# ===================================================================
st.subheader("Rail Station Ridership Heatmap, January - October 2024")

density_df = growth_df[
    (growth_df["transaction_year"] == 2024)
    & (growth_df["latitude"].notna())
    & (growth_df["longitude"].notna())
].copy()
density_df = density_df.drop_duplicates(subset=["station_name"])
density_df["passages_fmt"] = density_df["annual_passages"].apply(
    lambda x: f"{x:,.0f}"
)

st.markdown(
    "Ridership concentrates along the Bosphorus strait and the east-west Marmaray "
    "rail corridor. The densest clusters form around interchange stations that connect "
    "multiple lines: Yenikapi (M1/M2/Marmaray), Taksim (M2/F1), Kadikoy (M4/ferry), "
    "and Uskudar (M5/Marmaray/ferry). The Asian side shows a more dispersed pattern "
    "compared to the compact European core."
)

heatmap_layer = pdk.Layer(
    "HeatmapLayer",
    data=density_df,
    get_position=["longitude", "latitude"],
    get_weight="annual_passages",
    radius_pixels=60,
    intensity=1,
    threshold=0.05,
    opacity=0.8,
)

scatter_overlay = pdk.Layer(
    "ScatterplotLayer",
    data=density_df,
    get_position=["longitude", "latitude"],
    get_radius=120,
    get_fill_color=[255, 255, 255, 100],
    pickable=True,
)

view_density = pdk.ViewState(latitude=41.01, longitude=29.0, zoom=11, pitch=0)

st.pydeck_chart(
    pdk.Deck(
        layers=[heatmap_layer, scatter_overlay],
        initial_view_state=view_density,
        tooltip={
            "html": (
                "<b>{station_name}</b><br>"
                "Line: {line}<br>"
                "2024 Passages: {passages_fmt}"
            ),
            "style": {
                "backgroundColor": "#1a1a2e",
                "color": "white",
                "fontSize": "13px",
                "padding": "8px 12px",
            },
        },
        map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
    ),
    height=500,
)

st.markdown(
    map_legend_html([
        ("#FFFFFF", "Station location (white dot)"),
        ("#FF6B6B", "High ridership density (warm glow)"),
        ("#4444FF", "Low ridership density (cool glow)"),
    ]),
    unsafe_allow_html=True,
)

st.caption(
    "**Source:** IBB Rail Station Ridership Dataset — daily station-level "
    "passage counts with geographic coordinates (2024).  \n"
    "**Tools:** PyDeck HeatmapLayer weighted by annual passages, with "
    "ScatterplotLayer overlay for individual station tooltips.  \n"
    "**Methodology:** Heatmap weighted by annual_passages per station "
    "(radius_pixels=60, intensity=1, threshold=0.05). "
    "White scatter dots mark individual station locations.  \n"
    "**Limitations:** Only rail stations with valid coordinates are shown; "
    "bus stops and ferry piers are excluded. 2024 data covers Jan-Oct only. "
    "The color gradient is relative, not absolute — it shows where ridership "
    "concentrates within the rail network."
)

st.divider()

# ===================================================================
# Methodology
# ===================================================================
st.subheader("Data Sources and Methodology")
st.markdown("""
**Sources:**
- [Hourly Public Transport Data](https://data.ibb.gov.tr/en/dataset/hourly-public-transport-data-set) — Istanbulkart tap data, 60 monthly CSVs (Jan 2020 - Oct 2024)
- [Rail Station Ridership](https://data.ibb.gov.tr/en/dataset/rayli-sistemler-istasyon-bazli-yolcu-ve-yolculuk-sayilari) — Daily station-level data with coordinates (2021-2025)
- [Rail Ridership by Age Group](https://data.ibb.gov.tr/en/dataset/yas-grubuna-gore-rayli-sistemler-istasyon-bazli-yolcu-ve-yolculuk-sayilari) — Segmented by Istanbulkart registration age (2021-2025)
- [Ferry Pier Passengers](https://data.ibb.gov.tr/en/dataset/istanbul-deniz-iskeleleri-yolcu-sayilari) — Monthly pier-level ridership (2021-2025)
- [Rail Station GeoJSON](https://data.ibb.gov.tr/en/dataset/rayli-sistem-istasyon-noktalari-verisi) — Station points and construction status (June 2025)
- [Traffic Index](https://data.ibb.gov.tr/en/dataset/istanbul-trafik-indeksi) — Daily congestion index (2015-2024)

All data from the Istanbul Metropolitan Municipality (IBB) Open Data Portal under the IBB Open Data License.

**Tools:** Python, BigQuery, Bruin data pipeline, Streamlit, Altair, PyDeck. \
All charts use the [IBM Design Language Color Blind Safe palette](https://www.ibm.com/design/language/color/). \
Line charts use stroke dash patterns as a secondary encoding alongside color. \
Heatmaps use a sequential blue scale. Map legends are provided as labeled color swatches.

**Processing:**
- 718 million rows of hourly transport data loaded incrementally in 6-month batches via CKAN API
- Station coordinates in 2023/2025 data corrected for Turkish locale formatting (dots as thousands separators)
- 2021 age group data is monthly (not daily) and excluded from YoY comparisons
- Semicolon-delimited CSVs in 2023/2025 auto-detected and handled
- **Station phase corrections:** The IBB Rail Station GeoJSON is a snapshot from circa early 2023 and lists stations opened after that date as "under construction." This dashboard applies post-load corrections to reclassify stations confirmed operational by 2026, based on IBB press releases, Wikipedia, and news sources. Affected lines: F4 (opened Oct 2022), M3 Bakırköy extension (Mar 2024) and Yıldıztepe (Oct 2025), M5 Sancaktepe extension (Mar 2024), M9 MASKO (May 2021) and Ataköy extension (2024). Corrections are applied in the dashboard display layer only — the underlying BigQuery table retains the original IBB values
- **Station ordering:** M11 uses a manually defined station order to handle its inverted-V geometry (Kağıthane → Airport → Halkalı). M4 Tuzla branch separated from the main trunk to prevent Y-fork rendering artifacts. Alibeyköy Cep Otogarı mapped to T5 (IBB data lists it under a non-standard line name)

**Limitations:**
- 2022 rail station data shows anomalously high ridership (1.55B vs ~1.0-1.1B in other years) — likely a data collection methodology change
- 2024 hourly transport data ends October 18 (November-December files are empty placeholders on the portal)
- "Unknown" age group (23% of 2024 ridership) represents unregistered Istanbulkart holders
- The IBB GeoJSON station dataset is a static snapshot; station openings between 2022-2025 required manual phase corrections (see Processing above)
- Rail line paths are approximate straight segments between stations, not official rail alignments
""")
