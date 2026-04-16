from pathlib import Path

import altair as alt
import numpy as np
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
MODE_NAMES = {"OTOYOL": "Bus & Road", "RAYLI": "Rail", "DENİZ": "Ferry"}
MODE_COLORS = {"Bus & Road": PALETTE[0], "Rail": PALETTE[2], "Ferry": PALETTE[4]}


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
    "This dashboard analyzes **9.4 billion** Istanbulkart tap records across Istanbul's "
    "bus, metro, ferry, and Marmaray networks from January 2020 through October 2024. "
    "The data covers **718 million** rows of hourly transport data, **1.9 million** "
    "station-level rail records across **346 stations** on **23 lines**, and ferry "
    "ridership across **73 piers** operated by 12 companies. All data sourced from the "
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
# SECTION 1: COVID crash and recovery by mode
# ===================================================================
st.subheader("All three transit modes crashed 81-88% in April 2020 — rail recovered fastest")

monthly["mode"] = monthly["road_type"].map(MODE_NAMES)
monthly["passages_millions"] = monthly["monthly_passages"] / 1e6

mode_order = ["Bus & Road", "Rail", "Ferry"]
selection = alt.selection_point(fields=["mode"], bind="legend")

line_chart = (
    alt.Chart(monthly)
    .mark_line(strokeWidth=2)
    .encode(
        x=alt.X("month_date:T", title="Month"),
        y=alt.Y("passages_millions:Q", title="Monthly Passages (Millions)", scale=alt.Scale(zero=True)),
        color=alt.Color(
            "mode:N",
            title="Transit Mode",
            scale=alt.Scale(domain=mode_order, range=[MODE_COLORS[m] for m in mode_order]),
            sort=mode_order,
        ),
        opacity=alt.condition(selection, alt.value(1), alt.value(0.15)),
        tooltip=[
            alt.Tooltip("mode:N", title="Mode"),
            alt.Tooltip("month_date:T", title="Month", format="%b %Y"),
            alt.Tooltip("passages_millions:Q", title="Passages (M)", format=",.1f"),
        ],
    )
    .add_params(selection)
    .properties(height=380)
)

# COVID lockdown reference line
covid_rule = (
    alt.Chart(pd.DataFrame({"date": [pd.Timestamp("2020-04-01")]}))
    .mark_rule(color="#999999", strokeDash=[4, 4])
    .encode(x="date:T")
)
covid_label = (
    alt.Chart(pd.DataFrame({"date": [pd.Timestamp("2020-04-01")], "label": ["COVID Lockdown"]}))
    .mark_text(align="left", dx=5, dy=-170, fontSize=11, color="#999999")
    .encode(x="date:T", text="label:N")
)

st.altair_chart(line_chart + covid_rule + covid_label, use_container_width=True)

# Compute COVID recovery stats
pre_covid = monthly[monthly["month_date"] < "2020-03-01"].groupby("mode")["monthly_passages"].mean()
april_2020 = monthly[monthly["month_date"].dt.to_period("M") == "2020-04"].set_index("mode")["monthly_passages"]
crash_pct = ((april_2020 / pre_covid - 1) * 100).round(1)

st.markdown(
    f"> Bus ridership crashed **{crash_pct.get('Bus & Road', -81):.0f}%**, rail **{crash_pct.get('Rail', -87):.0f}%**, "
    f"and ferry **{crash_pct.get('Ferry', -88):.0f}%** in April 2020. "
    "By 2023, bus and rail exceeded their 2020 baseline but ferry plateaued. "
    "Road traffic congestion, meanwhile, surpassed pre-COVID levels by 2023 "
    "(traffic index 31-34 vs pre-COVID 29-31), suggesting a lasting modal shift toward private vehicles. "
    "Click legend entries to isolate individual modes."
)

st.divider()

# ===================================================================
# SECTION 2: Daily transit rhythm — heatmap
# ===================================================================
st.subheader("Rail peaks at 6 PM, buses at 7 AM — each mode serves a different rhythm")

hourly = run_query("hourly_heatmap.sql")
hourly["mode"] = hourly["road_type"].map(MODE_NAMES)

# Normalize within each mode to show relative patterns
hourly["normalized"] = hourly.groupby("mode")["avg_passages"].transform(lambda x: x / x.max())

# Fix day ordering: BigQuery DAYOFWEEK 1=Sun, 2=Mon, ..., 7=Sat
day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

# Build 3 heatmaps side by side
col_left, col_mid, col_right = st.columns(3)
for col, mode_name in zip([col_left, col_mid, col_right], ["Rail", "Bus & Road", "Ferry"]):
    mode_data = hourly[hourly["mode"] == mode_name].copy()
    with col:
        heat = (
            alt.Chart(mode_data)
            .mark_rect(cornerRadius=2)
            .encode(
                x=alt.X("transition_hour:O", title="Hour", axis=alt.Axis(labelAngle=0)),
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
                    alt.Tooltip("avg_passages:Q", title="Avg Passages", format=",.0f"),
                ],
            )
            .properties(height=220, title=mode_name)
        )
        st.altair_chart(heat, use_container_width=True)

st.markdown(
    "> Rail shows a pronounced evening peak at **18:00** — 23% higher than the morning peak (08:00). "
    "Bus ridership has a more symmetric pattern with **07:00** as the single busiest hour. "
    "Ferry peaks in late afternoon and is **stronger on weekends** than weekdays at 17:00, "
    "reflecting its role in tourism and leisure travel. "
    "All modes see approximately **5x** more ridership on weekdays than weekends."
)

st.divider()

# ===================================================================
# SECTION 3: Metro expansion — 3D station map
# ===================================================================
st.subheader("59 new rail stations opened in 2024 — the largest expansion in recent history")

growth_df = run_query("station_growth_map.sql")
stations_2024 = growth_df[growth_df["transaction_year"] == 2024].copy()
stations_2024 = stations_2024.drop_duplicates(subset=["station_name", "line"])

# Color by growth rate (diverging: blue=growth, orange=decline, grey=new/no data)
def growth_to_color(row):
    pct = row.get("yoy_growth_pct")
    if pd.isna(pct):
        return [0, 158, 115, 200]  # green for new stations
    if pct > 50:
        return [0, 114, 178, 220]
    if pct > 10:
        return [86, 180, 233, 200]
    if pct > -10:
        return [153, 153, 153, 180]
    if pct > -50:
        return [230, 159, 0, 200]
    return [213, 94, 0, 220]

stations_2024["color"] = stations_2024.apply(growth_to_color, axis=1)

# Height proportional to ridership (log scale for visual balance)
stations_2024["elevation"] = np.log10(stations_2024["annual_passages"].clip(lower=10)) * 600

column_layer = pdk.Layer(
    "ColumnLayer",
    data=stations_2024,
    get_position=["longitude", "latitude"],
    get_elevation="elevation",
    elevation_scale=1,
    get_fill_color="color",
    radius=180,
    pickable=True,
    auto_highlight=True,
    extruded=True,
)

view_3d = pdk.ViewState(
    latitude=41.02,
    longitude=29.0,
    zoom=10.3,
    pitch=50,
    bearing=-15,
)

st.pydeck_chart(
    pdk.Deck(
        layers=[column_layer],
        initial_view_state=view_3d,
        tooltip={
            "html": "<b>{station_name}</b><br>Line: {line}<br>"
                    "2024 Passages: {annual_passages}<br>"
                    "YoY Growth: {yoy_growth_pct}%",
            "style": {"backgroundColor": "#1a1a2e", "color": "white", "fontSize": "12px"},
        },
        map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
    ),
    height=550,
)

col_legend1, col_legend2, col_legend3 = st.columns(3)
with col_legend1:
    st.markdown("**Blue** = Growing stations (>10% YoY)")
with col_legend2:
    st.markdown("**Orange** = Declining stations (<-10% YoY)")
with col_legend3:
    st.markdown("**Green** = New stations (no prior year data)")

# Top growth table
top_growth = (
    stations_2024[stations_2024["yoy_growth_pct"].notna() & (stations_2024["annual_passages"] > 50000)]
    .nlargest(10, "yoy_growth_pct")[["station_name", "line", "town", "yoy_growth_pct", "annual_passages"]]
    .rename(columns={
        "station_name": "Station",
        "line": "Line",
        "town": "District",
        "yoy_growth_pct": "YoY Growth %",
        "annual_passages": "2024 Passages",
    })
)
st.markdown("**Top 10 fastest-growing stations (2023 to 2024, min 50K passages):**")
st.dataframe(top_growth, hide_index=True, use_container_width=True)

st.markdown(
    "> The M5 Uskudar-Cekmekoy extension on the Asian side drove the largest growth, "
    "with stations like Dudullu (+242%), Cekmekoy 1 (+438%), and Necip Fazil (+114%). "
    "The new airport rail link (Gayrettepe-Istanbul Havalimani) carried 13M passengers "
    "in its first year. Column height represents ridership volume (log scale). "
    "Several T1 tram stations show 90%+ apparent declines, likely from line restructuring."
)

st.divider()

# ===================================================================
# SECTION 4: Network coverage and underserved districts
# ===================================================================
st.subheader("268 stations serve the city today, with 73 more under construction")

geo_df = run_query("geo_stations.sql")

# Separate existing and construction
existing = geo_df[geo_df["project_phase"] == "Mevcut Hattaki \u0130stasyon"].copy()
construction = geo_df[geo_df["project_phase"] == "\u0130n\u015faat A\u015famas\u0131nda"].copy()

existing_count = len(existing)
construction_count = len(construction)

layer_existing = pdk.Layer(
    "ScatterplotLayer",
    data=existing,
    get_position=["longitude", "latitude"],
    get_radius=250,
    get_fill_color=[0, 158, 115, 220],
    pickable=True,
    auto_highlight=True,
)
layer_construction = pdk.Layer(
    "ScatterplotLayer",
    data=construction,
    get_position=["longitude", "latitude"],
    get_radius=250,
    get_fill_color=[213, 94, 0, 220],
    pickable=True,
    auto_highlight=True,
    stroked=True,
    get_line_color=[255, 255, 255, 100],
    line_width_min_pixels=1,
)

view_network = pdk.ViewState(latitude=41.02, longitude=29.0, zoom=10.5, pitch=0)

st.pydeck_chart(
    pdk.Deck(
        layers=[layer_existing, layer_construction],
        initial_view_state=view_network,
        tooltip={
            "html": "<b>{station_name}</b><br>{line_name}<br>Type: {line_type}<br>{project_phase}",
            "style": {"backgroundColor": "#1a1a2e", "color": "white", "fontSize": "12px"},
        },
        map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
    ),
    height=500,
)

col_a, col_b = st.columns(2)
with col_a:
    st.markdown(f"**Green** = Existing stations ({existing_count})")
with col_b:
    st.markdown(f"**Orange** = Under construction ({construction_count})")

# District underserved analysis
district_df = run_query("district_ridership.sql")

st.markdown("**District ridership and rail coverage (2023):**")

# Show top districts sorted by total passages, highlighting rail share
district_display = district_df[["town", "total_passages", "rail_passages", "bus_passages", "ferry_passages", "rail_share_pct"]].copy()
district_display.columns = ["District", "Total Passages", "Rail", "Bus", "Ferry", "Rail Share %"]
district_display = district_display.sort_values("Total Passages", ascending=False).head(20)
st.dataframe(district_display, hide_index=True, use_container_width=True)

# Underserved districts (no rail or low rail share)
no_rail = district_df[(district_df["rail_share_pct"] == 0) | (district_df["rail_share_pct"].isna())]
no_rail_list = no_rail.sort_values("total_passages", ascending=False).head(8)

if not no_rail_list.empty:
    districts_text = ", ".join(
        f"**{r['town']}** ({r['total_passages'] / 1e6:.1f}M trips)"
        for _, r in no_rail_list.iterrows()
    )
    st.markdown(
        f"> **Rail deserts:** {districts_text} — "
        "these districts have significant bus ridership but zero rail access. "
        "The 73 stations under construction target underserved areas, "
        "particularly on the Asian side and northern European side."
    )

st.divider()

# ===================================================================
# SECTION 5: Ridership density — heatmap layer
# ===================================================================
st.subheader("Ridership concentrates along the Bosphorus corridor and Marmaray crossings")

# Use station growth data for density visualization
density_df = growth_df[
    (growth_df["transaction_year"] == 2024)
    & (growth_df["latitude"].notna())
    & (growth_df["longitude"].notna())
].copy()
density_df = density_df.drop_duplicates(subset=["station_name"])

# HeatmapLayer for a glowing density effect
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

# Scatter overlay showing individual stations
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
            "html": "<b>{station_name}</b><br>Line: {line}<br>2024 Passages: {annual_passages}",
            "style": {"backgroundColor": "#1a1a2e", "color": "white", "fontSize": "12px"},
        },
        map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
    ),
    height=500,
)

st.markdown(
    "> The ridership heatmap reveals a clear concentration along the Bosphorus strait "
    "and the east-west Marmaray corridor. High-density clusters form around major "
    "interchange stations: Yenikapi (M1/M2/Marmaray), Taksim (M2/F1), Kadikoy (M4/ferry), "
    "and Uskudar (M5/Marmaray/ferry). The Asian side shows more dispersed ridership "
    "patterns compared to the compact European core."
)

st.divider()

# ===================================================================
# Methodology
# ===================================================================
st.subheader("Methodology")
st.markdown("""
**Data sources:**
- [Hourly Public Transport Data](https://data.ibb.gov.tr/en/dataset/hourly-public-transport-data-set) — Istanbulkart tap data, 60 monthly CSVs (Jan 2020 - Oct 2024, ~60 GB total)
- [Rail Station Ridership](https://data.ibb.gov.tr/en/dataset/rayli-sistemler-istasyon-bazli-yolcu-ve-yolculuk-sayilari) — Daily station-level data with coordinates (2021-2025)
- [Rail Ridership by Age Group](https://data.ibb.gov.tr/en/dataset/yas-grubuna-gore-rayli-sistemler-istasyon-bazli-yolcu-ve-yolculuk-sayilari) — Segmented by Istanbulkart registration age (2021-2025)
- [Ferry Pier Passengers](https://data.ibb.gov.tr/en/dataset/istanbul-deniz-iskeleleri-yolcu-sayilari) — Monthly pier-level ridership (2021-2025)
- [Rail Station GeoJSON](https://data.ibb.gov.tr/en/dataset/rayli-sistem-istasyon-noktalari-verisi) — Station points and construction status (June 2025)
- [Traffic Index](https://data.ibb.gov.tr/en/dataset/istanbul-trafik-indeksi) — Daily congestion index (2015-2024)

All data from the Istanbul Metropolitan Municipality (IBB) Open Data Portal under the IBB Open Data License.

**Processing:**
- 718 million rows of hourly transport data loaded incrementally in 6-month batches via CKAN API
- Station coordinates in 2023/2025 data corrected for Turkish locale formatting (dots as thousands separators)
- 2021 age group data is monthly (not daily) and excluded from YoY comparisons
- Semicolon-delimited CSVs in 2023/2025 auto-detected and handled

**Limitations:**
- 2022 rail station data shows anomalously high ridership (1.55B vs ~1.0-1.1B in other years) — likely a data collection methodology change
- 2024 hourly transport data ends October 18 (November-December files are empty placeholders on the portal)
- "Unknown" age group (23% of 2024 ridership) represents unregistered Istanbulkart holders
- Station growth figures showing >90% decline on T1/T5 tram lines likely reflect line restructuring, not actual ridership collapse
- District coordinates are approximate centroids from rail station locations; districts without rail have no map coordinates
""")
