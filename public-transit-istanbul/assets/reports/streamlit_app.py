from pathlib import Path

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(page_title="Istanbul Public Transit", layout="wide")

PROJECT_ID = "bruin-playground-arsalan"
base_path = Path(__file__).parent

# Wong (2011) colorblind-safe palette
PALETTE = ["#0072B2", "#D55E00", "#56B4E9", "#E69F00", "#009E73", "#CC79A7", "#F0E442", "#999999"]


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


# --- Header ---
st.title("Istanbul Public Transit: Ridership & Metro Expansion")
st.markdown(
    "Rail ridership, ferry trends, station growth, and rider demographics "
    "across Istanbul's public transit network (2021-2025)."
)

# --- KPI Metrics ---
rail_yearly = run_query("rail_ridership_by_year.sql")
ferry_yearly = run_query("ferry_trends.sql")

latest_rail = rail_yearly[rail_yearly["transaction_year"] == 2024].iloc[0]
prev_rail = rail_yearly[rail_yearly["transaction_year"] == 2023].iloc[0]
latest_ferry = ferry_yearly[ferry_yearly["year"] == 2024].iloc[0]

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric(
        "Rail Passages (2024)",
        f"{latest_rail['total_passages'] / 1e9:.2f}B",
        delta=f"{(latest_rail['total_passages'] - prev_rail['total_passages']) / prev_rail['total_passages'] * 100:.1f}%",
    )
with col2:
    st.metric(
        "Active Rail Stations (2024)",
        int(latest_rail["active_stations"]),
        delta=f"+{int(latest_rail['active_stations'] - prev_rail['active_stations'])} new",
    )
with col3:
    st.metric(
        "Ferry Journeys (2024)",
        f"{latest_ferry['total_journeys'] / 1e6:.1f}M",
    )
with col4:
    st.metric(
        "Active Rail Lines (2024)",
        int(latest_rail["active_lines"]),
        delta=f"+{int(latest_rail['active_lines'] - prev_rail['active_lines'])} new",
    )

st.divider()

# ===================================================================
# CHART 1: Rail ridership grew 93% from 2021 to 2024
# ===================================================================
st.subheader("Rail ridership grew 93% from 2021 to 2024 as new lines opened")

rail_yearly["passages_billions"] = rail_yearly["total_passages"] / 1e9

bar = (
    alt.Chart(rail_yearly)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, color=PALETTE[0])
    .encode(
        x=alt.X("transaction_year:O", title="Year"),
        y=alt.Y("passages_billions:Q", title="Total Passages (Billions)", scale=alt.Scale(zero=True)),
        tooltip=[
            alt.Tooltip("transaction_year:O", title="Year"),
            alt.Tooltip("passages_billions:Q", title="Passages (B)", format=".2f"),
            alt.Tooltip("active_stations:Q", title="Stations"),
            alt.Tooltip("active_lines:Q", title="Lines"),
        ],
    )
    .properties(height=380)
)

stations_line = (
    alt.Chart(rail_yearly)
    .mark_text(dy=-12, fontSize=12, fontWeight="bold", color=PALETTE[1])
    .encode(
        x=alt.X("transaction_year:O"),
        y=alt.Y("passages_billions:Q"),
        text=alt.Text("active_stations:Q"),
    )
)

st.altair_chart(bar + stations_line, use_container_width=True)
st.markdown(
    "> Rail passages rose from **588M** in 2021 to **1.13B** in 2024 (+93%). "
    "The network expanded from 274 to 346 active stations (+72 stations). "
    "2022 shows an anomaly at 1.55B — likely data methodology differences for that year. "
    "Numbers above bars show active station counts."
)

st.divider()

# ===================================================================
# CHART 2: Top rail lines by ridership
# ===================================================================
st.subheader("M2 Yenikapi-Haciosman carries the most passengers across all years")

lines_df = run_query("top_rail_lines.sql")

# Take top 8 lines by 2024 ridership
top_lines_2024 = (
    lines_df[lines_df["transaction_year"] == 2024]
    .nlargest(8, "total_passages")["line"]
    .tolist()
)
top_lines_data = lines_df[lines_df["line"].isin(top_lines_2024)].copy()
top_lines_data["passages_millions"] = top_lines_data["total_passages"] / 1e6
# Shorten line names for readability
top_lines_data["line_short"] = top_lines_data["line"].str.split("-", n=1).str[0].str.strip()

selection = alt.selection_point(fields=["line_short"], bind="legend")

line_chart = (
    alt.Chart(top_lines_data)
    .mark_line(point=True, strokeWidth=2)
    .encode(
        x=alt.X("transaction_year:O", title="Year"),
        y=alt.Y("passages_millions:Q", title="Passages (Millions)", scale=alt.Scale(zero=True)),
        color=alt.Color(
            "line_short:N",
            title="Line",
            scale=alt.Scale(range=PALETTE),
        ),
        opacity=alt.condition(selection, alt.value(1), alt.value(0.15)),
        tooltip=[
            alt.Tooltip("line:N", title="Line"),
            alt.Tooltip("transaction_year:O", title="Year"),
            alt.Tooltip("passages_millions:Q", title="Passages (M)", format=",.0f"),
            alt.Tooltip("stations:Q", title="Stations"),
        ],
    )
    .add_params(selection)
    .properties(height=380)
)

st.altair_chart(line_chart, use_container_width=True)
st.markdown(
    "> **M2 Yenikapi-Haciosman** leads all lines with 225M passages in 2024, "
    "followed by M4 Kadikoy-Sabiha Gokcen (183M) and Marmaray (145M). "
    "Click legend entries to isolate individual lines."
)
st.caption("Note: Year-to-year fluctuations may reflect data collection methodology changes.")

st.divider()

# ===================================================================
# CHART 3: Station growth map
# ===================================================================
st.subheader("New metro lines on the Asian side drove the largest station ridership surges")

growth_df = run_query("station_growth_map.sql")

# Filter to years with growth data
growth_latest = growth_df[
    (growth_df["transaction_year"] == 2024)
    & (growth_df["yoy_growth_pct"].notna())
    & (growth_df["prev_year_passages"] > 50000)
].copy()

# Drop exact duplicates (some stations appear multiple times)
growth_latest = growth_latest.drop_duplicates(subset=["station_name", "line"])

# Classify growth
growth_latest["growth_category"] = pd.cut(
    growth_latest["yoy_growth_pct"],
    bins=[-1000, -50, -10, 10, 50, 1000],
    labels=["Declined >50%", "Declined 10-50%", "Stable", "Grew 10-50%", "Grew >50%"],
)

# Color scale
color_map = {
    "Declined >50%": [213, 94, 0, 200],
    "Declined 10-50%": [230, 159, 0, 180],
    "Stable": [153, 153, 153, 150],
    "Grew 10-50%": [86, 180, 233, 180],
    "Grew >50%": [0, 114, 178, 200],
}
growth_latest["color"] = growth_latest["growth_category"].map(color_map)
growth_latest["radius"] = (growth_latest["annual_passages"] / growth_latest["annual_passages"].max() * 800 + 200).clip(200, 1000)

# PyDeck map
view_state = pdk.ViewState(
    latitude=41.015,
    longitude=29.0,
    zoom=10,
    pitch=0,
)

layer = pdk.Layer(
    "ScatterplotLayer",
    data=growth_latest,
    get_position=["longitude", "latitude"],
    get_radius="radius",
    get_fill_color="color",
    pickable=True,
    auto_highlight=True,
)

tooltip = {
    "html": "<b>{station_name}</b><br>Line: {line}<br>Growth: {yoy_growth_pct}%<br>2024 Passages: {annual_passages}",
    "style": {"backgroundColor": "#333", "color": "white", "fontSize": "12px"},
}

st.pydeck_chart(
    pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip=tooltip,
        map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
    )
)

# Growth leaders table
st.markdown("**Top 10 growth stations (2023 to 2024):**")
top_growth = (
    growth_latest[growth_latest["yoy_growth_pct"] > 0]
    .nlargest(10, "yoy_growth_pct")[["station_name", "line", "town", "yoy_growth_pct", "annual_passages"]]
    .rename(columns={
        "station_name": "Station",
        "line": "Line",
        "town": "District",
        "yoy_growth_pct": "YoY Growth %",
        "annual_passages": "2024 Passages",
    })
)
st.dataframe(top_growth, hide_index=True, use_container_width=True)

st.markdown(
    "> M5 Uskudar-Cekmekoy metro stations on the Asian side saw the largest gains, "
    "with Dudullu growing **+242%** year-over-year. Meanwhile, several Fatih district "
    "stations along the T1 tram declined over 90% — likely due to line restructuring. "
    "Blue = growing, orange = declining. Circle size = ridership volume."
)

st.divider()

# ===================================================================
# CHART 4: Rider demographics by age group
# ===================================================================
st.subheader("Working-age adults (20-60) account for 57% of rail ridership")

age_df = run_query("age_group_trends.sql")
# Standardize age group names
age_df["age_group"] = age_df["age_group"].replace({"Unkown": "Unknown"})
age_df["passages_millions"] = age_df["total_passages"] / 1e6

# Exclude 2021 (very low counts, different methodology)
age_df = age_df[age_df["transaction_year"] >= 2022]

age_order = ["<20", "20-30", "30-60", "60+", "Unknown"]

age_selection = alt.selection_point(fields=["age_group"], bind="legend")

age_chart = (
    alt.Chart(age_df)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    .encode(
        x=alt.X("transaction_year:O", title="Year"),
        y=alt.Y("passages_millions:Q", title="Passages (Millions)", stack="zero", scale=alt.Scale(zero=True)),
        color=alt.Color(
            "age_group:N",
            title="Age Group",
            scale=alt.Scale(domain=age_order, range=PALETTE[:5]),
            sort=age_order,
        ),
        opacity=alt.condition(age_selection, alt.value(1), alt.value(0.2)),
        tooltip=[
            alt.Tooltip("age_group:N", title="Age Group"),
            alt.Tooltip("transaction_year:O", title="Year"),
            alt.Tooltip("passages_millions:Q", title="Passages (M)", format=",.0f"),
        ],
    )
    .add_params(age_selection)
    .properties(height=380)
)

st.altair_chart(age_chart, use_container_width=True)

# 2024 breakdown
age_2024 = age_df[age_df["transaction_year"] == 2024].copy()
age_2024["pct"] = (age_2024["total_passages"] / age_2024["total_passages"].sum() * 100).round(1)
age_2024 = age_2024[["age_group", "total_passages", "pct"]].rename(columns={
    "age_group": "Age Group",
    "total_passages": "2024 Passages",
    "pct": "Share %",
})
st.dataframe(age_2024, hide_index=True, use_container_width=True)

st.markdown(
    "> The 30-60 age group dominates rail ridership at **33%** of all passages in 2024, "
    "followed by 20-30 year olds at **24%**. Riders over 60 account for 13%. "
    "The \"Unknown\" category (23%) includes unregistered Istanbulkart holders. "
    "Click legend entries to isolate age groups."
)

st.divider()

# ===================================================================
# CHART 5: Ferry ridership recovery
# ===================================================================
st.subheader("Ferry ridership recovered from COVID and stabilized at 70-72M journeys/year")

ferry_yearly["journeys_millions"] = ferry_yearly["total_journeys"] / 1e6

ferry_bar = (
    alt.Chart(ferry_yearly)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, color=PALETTE[4])
    .encode(
        x=alt.X("year:O", title="Year"),
        y=alt.Y("journeys_millions:Q", title="Total Journeys (Millions)", scale=alt.Scale(zero=True)),
        tooltip=[
            alt.Tooltip("year:O", title="Year"),
            alt.Tooltip("journeys_millions:Q", title="Journeys (M)", format=",.1f"),
            alt.Tooltip("active_piers:Q", title="Active Piers"),
            alt.Tooltip("trips_per_person:Q", title="Trips/Person", format=".2f"),
        ],
    )
    .properties(height=380)
)

trips_text = (
    alt.Chart(ferry_yearly)
    .mark_text(dy=-12, fontSize=11, fontWeight="bold", color=PALETTE[1])
    .encode(
        x=alt.X("year:O"),
        y=alt.Y("journeys_millions:Q"),
        text=alt.Text("trips_per_person:Q", format=".2f"),
    )
)

st.altair_chart(ferry_bar + trips_text, use_container_width=True)
st.markdown(
    "> Ferry journeys surged from **47M** in 2021 to **72M** in 2022 (+52%), "
    "then plateaued at 70-72M through 2025. Average trips per unique passenger "
    "increased from 1.64 to 1.84, indicating more regular commuter usage. "
    "Numbers above bars show average trips per person."
)

st.divider()

# ===================================================================
# Station map (all stations from GeoJSON)
# ===================================================================
st.subheader("Istanbul's rail network: 268 existing stations, 75 under construction")

geo_df = run_query("geo_stations.sql")

phase_color = {
    "Mevcut Hattaki \u0130stasyon": [0, 158, 115, 200],  # existing = green
    "\u0130n\u015faat A\u015famas\u0131nda": [213, 94, 0, 200],  # construction = orange
}
geo_df["color"] = geo_df["project_phase"].map(phase_color).fillna([153, 153, 153, 150])

existing = geo_df[geo_df["project_phase"] == "Mevcut Hattaki \u0130stasyon"]
construction = geo_df[geo_df["project_phase"] == "\u0130n\u015faat A\u015famas\u0131nda"]

layer_existing = pdk.Layer(
    "ScatterplotLayer",
    data=existing,
    get_position=["longitude", "latitude"],
    get_radius=200,
    get_fill_color=[0, 158, 115, 200],
    pickable=True,
)
layer_construction = pdk.Layer(
    "ScatterplotLayer",
    data=construction,
    get_position=["longitude", "latitude"],
    get_radius=200,
    get_fill_color=[213, 94, 0, 200],
    pickable=True,
)

view_full = pdk.ViewState(latitude=41.02, longitude=29.0, zoom=10.5, pitch=0)

st.pydeck_chart(
    pdk.Deck(
        layers=[layer_existing, layer_construction],
        initial_view_state=view_full,
        tooltip={"html": "<b>{station_name}</b><br>{line_name}<br>Type: {line_type}<br>{project_phase}"},
        map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
    )
)

col_a, col_b = st.columns(2)
with col_a:
    st.markdown("**Green** = Existing stations (268)")
with col_b:
    st.markdown("**Orange** = Under construction (75)")

st.markdown(
    "> Istanbul has 73 metro stations currently under construction, plus 2 new funicular stations. "
    "The metro expansion is concentrated on the Asian side and northern European side, "
    "aiming to connect underserved districts to the core network."
)

st.divider()

# ===================================================================
# Methodology
# ===================================================================
st.subheader("Methodology")
st.markdown("""
**Data sources:**
- [Hourly Public Transport Data](https://data.ibb.gov.tr/en/dataset/hourly-public-transport-data-set) — Istanbulkart tap data (2020-2024, monthly CSVs)
- [Rail Station Ridership](https://data.ibb.gov.tr/en/dataset/rayli-sistemler-istasyon-bazli-yolcu-ve-yolculuk-sayilari) — Daily station-level data with coordinates (2021-2025)
- [Rail Ridership by Age Group](https://data.ibb.gov.tr/en/dataset/yas-grubuna-gore-rayli-sistemler-istasyon-bazli-yolcu-ve-yolculuk-sayilari) — Segmented by Istanbulkart registration age (2021-2025)
- [Ferry Pier Passengers](https://data.ibb.gov.tr/en/dataset/istanbul-deniz-iskeleleri-yolcu-sayilari) — Monthly pier-level ridership (2021-2025)
- [Rail Station GeoJSON](https://data.ibb.gov.tr/en/dataset/rayli-sistem-istasyon-noktalari-verisi) — Station points and lines (June 2025)
- [Traffic Index](https://data.ibb.gov.tr/en/dataset/istanbul-trafik-indeksi) — Daily congestion index (2015-2024)

All data from the Istanbul Metropolitan Municipality (IBB) Open Data Portal under the IBB Open Data License.

**Limitations:**
- 2022 rail station data shows anomalously high ridership — likely a methodology change in that year's data collection
- 2021 age group data is monthly (not daily) and has only 17K rows vs 500K+ in other years
- Some station coordinates in 2023/2025 data used Turkish locale formatting (dots as thousands separators) — corrected during ingestion but ~33 stations still have invalid coordinates
- The hourly transport data (60 GB total) is loaded incrementally; dashboard coverage depends on which months have been backfilled
- "Unknown" age group (23% of 2024 ridership) represents unregistered Istanbulkart holders — demographics are based on voluntary registration only
""")
