from pathlib import Path

import altair as alt
import json
import numpy as np
import pandas as pd
import pydeck as pdk
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(page_title="City Pulse — Decoding Urban Form", layout="wide")

PROJECT_ID = "bruin-playground-arsalan"
base_path = Path(__file__).parent


@st.cache_resource
def get_client():
    credentials = service_account.Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)


def run_raw(sql: str) -> pd.DataFrame:
    return get_client().query(sql).to_dataframe()


# ── Load data ─────────────────────────────────────────────────────────

cities = run_raw("""
    SELECT
        ghsl_id, city_name, country_code, country_name, latitude, longitude,
        population_2015, population_2000, population_1975,
        area_km2, gdp_ppp, avg_building_height_m, hdi, avg_temp_c,
        precipitation_mm, elevation_m,
        population_tier, climate_zone, continent,
        pop_growth_pct_2000_2015, pop_density_per_km2,
        has_network_analysis, orientation_entropy, orientation_order,
        avg_street_length_m, intersection_count, dead_end_proportion,
        avg_circuity, bearing_counts
    FROM `bruin-playground-arsalan.staging.city_profiles`
    WHERE population_2015 > 0
    ORDER BY population_2015 DESC
""")

trends = run_raw("""
    SELECT
        country_code, country_name, year, urbanization_pct,
        urban_growth_rate, largest_city_pct, gdp_per_capita,
        total_population, pop_density,
        urbanization_stage, urbanization_velocity_5yr,
        decade, region, income_group, urban_population_est
    FROM `bruin-playground-arsalan.staging.urban_trends`
    WHERE urbanization_pct IS NOT NULL
    ORDER BY country_code, year
""")

# Cities with street network analysis
network_cities = cities[cities["has_network_analysis"]].copy()

# Shared data footnote
DATA_FOOTER = (
    "Source: <b>GHSL Urban Centre Database R2024A</b> (EU JRC, CC BY 4.0), "
    "<b>OpenStreetMap</b> via OSMnx (ODbL), "
    "<b>World Bank Open Data API</b> (CC BY 4.0)."
    "<br>"
    "Tools: <b>Bruin</b> (pipeline), <b>BigQuery</b> (warehouse), "
    "<b>OSMnx</b> + <b>NetworkX</b> (street analysis), "
    "<b>Altair</b> + <b>Pydeck</b> + <b>Matplotlib</b> (visualization)."
)

# High-contrast colorblind-safe palette (Wong 2011, adjusted for white bg)
VERMILLION = "#C84500"
SKY_BLUE = "#3A9AD9"
ORANGE = "#D48A00"
BLUE_GREEN = "#007A5E"
BLUE = "#0060A8"
PURPLE = "#A8507A"
DARK_GOLD = "#8A7A00"
MUTED = "#777777"

CONTINENT_COLORS = {
    "Asia": BLUE,
    "Europe": BLUE_GREEN,
    "Africa": VERMILLION,
    "Americas": PURPLE,
    "Oceania": ORANGE,
    "Other": MUTED,
}

CONTINENT_SHAPES = {
    "Asia": "square",
    "Europe": "circle",
    "Africa": "cross",
    "Americas": "diamond",
    "Oceania": "triangle-up",
    "Other": "triangle-down",
}

# Global Altair theme
alt.themes.register("city_pulse", lambda: {
    "config": {
        "legend": {
            "symbolSize": 266,
            "labelFontSize": 17,
            "titleFontSize": 19,
            "symbolStrokeWidth": 2,
            "labelLimit": 330,
            "columnPadding": 21,
            "rowPadding": 8,
        },
        "axis": {
            "titleFontSize": 13,
            "labelFontSize": 12,
        },
    }
})
alt.themes.enable("city_pulse")


# ── Helper: diverging color ───────────────────────────────────────────

def diverging_color(value, vmin, vmax, low_rgb=(58, 154, 217), high_rgb=(200, 69, 0)):
    """Map value to diverging color scale, return [R, G, B, A]."""
    if pd.isna(value):
        return [128, 128, 128, 100]
    t = (value - vmin) / (vmax - vmin) if vmax != vmin else 0.5
    t = max(0.0, min(1.0, t))
    mid = (255, 255, 255)
    if t < 0.5:
        s = t * 2
        r = int(low_rgb[0] + s * (mid[0] - low_rgb[0]))
        g = int(low_rgb[1] + s * (mid[1] - low_rgb[1]))
        b = int(low_rgb[2] + s * (mid[2] - low_rgb[2]))
    else:
        s = (t - 0.5) * 2
        r = int(mid[0] + s * (high_rgb[0] - mid[0]))
        g = int(mid[1] + s * (high_rgb[1] - mid[1]))
        b = int(mid[2] + s * (high_rgb[2] - mid[2]))
    return [r, g, b, 180]


# ── Header ────────────────────────────────────────────────────────────

st.title("City Pulse — Decoding Urban Form")
st.caption(
    f"Analyzing {len(cities):,} urban centers across {cities['country_code'].nunique()} countries "
    f"using GHSL, OpenStreetMap street network analysis, and World Bank data.  \u00b7  "
    "Pipeline: Bruin + BigQuery + OSMnx"
)

# KPIs
latest_year = int(trends["year"].max())
latest_trends = trends[trends["year"] == latest_year]

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Urban Centers Mapped", f"{len(cities):,}")
with col2:
    megacities = cities[cities["population_2015"] >= 10_000_000]
    st.metric("Megacities (10M+)", len(megacities))
with col3:
    if len(network_cities) > 0:
        most_grid = network_cities.loc[network_cities["orientation_order"].idxmax()]
        st.metric("Most Grid-Like", most_grid["city_name"],
                  delta=f"Order: {most_grid['orientation_order']:.2f}")
    else:
        st.metric("Most Grid-Like", "N/A")
with col4:
    # Most explosive growth city
    has_both = cities[(cities["population_1975"] > 10000) & (cities["population_2015"] > 100000)].copy()
    if len(has_both) > 0:
        has_both["growth_multiplier"] = has_both["population_2015"] / has_both["population_1975"]
        explosive = has_both.loc[has_both["growth_multiplier"].idxmax()]
        st.metric("Most Explosive Growth", explosive["city_name"],
                  delta=f"{explosive['growth_multiplier']:.0f}x since 1975")

st.markdown("---")

# ======================================================================
# CHART 1: Where the World Lives - Pydeck ScatterplotLayer
# ======================================================================

st.subheader("Where the world lives")
st.caption(
    f"All {len(cities):,} urban centers from the GHSL database. "
    "Circle size proportional to population. "
    "Color encodes selected metric. Dark basemap: CARTO Dark Matter."
)

metric_options = {
    "Population growth 2000-2015 (%)": "pop_growth_pct_2000_2015",
    "Avg building height (m)": "avg_building_height_m",
    "Human Development Index": "hdi",
    "Population density (per km2)": "pop_density_per_km2",
    "Elevation (m)": "elevation_m",
}

selected_metric_label = st.selectbox(
    "Color cities by:", list(metric_options.keys()), index=0
)
selected_metric = metric_options[selected_metric_label]

# Prepare map data
map_data = cities[["city_name", "country_name", "latitude", "longitude",
                    "population_2015", "population_tier", "continent",
                    selected_metric]].copy()
map_data = map_data.dropna(subset=["latitude", "longitude"])

# More exaggerated radius: power scale for dramatic size differences
map_data["radius"] = np.power(map_data["population_2015"].clip(lower=1), 0.45) * 15

# Compute fill color
col_vals = map_data[selected_metric].dropna()
if len(col_vals) > 0:
    vmin = col_vals.quantile(0.05)
    vmax = col_vals.quantile(0.95)
    map_data["fill_color"] = map_data[selected_metric].apply(
        lambda v: diverging_color(v, vmin, vmax)
    )
else:
    map_data["fill_color"] = [[128, 128, 128, 100]] * len(map_data)

# Format tooltip column
map_data["metric_display"] = map_data[selected_metric].apply(
    lambda v: f"{v:,.1f}" if pd.notna(v) else "N/A"
)
map_data["pop_display"] = map_data["population_2015"].apply(lambda v: f"{v:,.0f}")

scatter_layer = pdk.Layer(
    "ScatterplotLayer",
    data=map_data,
    get_position=["longitude", "latitude"],
    get_radius="radius",
    get_fill_color="fill_color",
    pickable=True,
    opacity=0.7,
    auto_highlight=True,
    radius_min_pixels=1,
    radius_max_pixels=100,
)

view_state = pdk.ViewState(
    latitude=20,
    longitude=15,
    zoom=1.5,
    pitch=0,
)

st.pydeck_chart(pdk.Deck(
    layers=[scatter_layer],
    initial_view_state=view_state,
    map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
    tooltip={
        "html": "<b>{city_name}</b>, {country_name}<br>"
                "Population: {pop_display}<br>"
                f"{selected_metric_label}: " + "{metric_display}",
        "style": {"backgroundColor": "#333", "color": "white"},
    },
))

# Megacity table
st.markdown(f"> **{len(megacities)} megacities** (population over 10 million) span "
            f"{megacities['continent'].nunique()} continents. "
            f"{len(megacities[megacities['continent'] == 'Asia'])} are in Asia.")

st.caption(DATA_FOOTER, unsafe_allow_html=True)

st.markdown("---")

# ======================================================================
# CHART 2: City Fingerprints - Street Orientation Polar Plots
# ======================================================================

st.subheader("City fingerprints: street orientation patterns")
st.caption(
    "Street bearing distributions for analyzed cities. Each plot shows how streets are oriented "
    "(36 bins, 10 degrees each). Grid cities show sharp directional spikes; organic cities show "
    "uniform distributions. Order score: 0 = random, 1 = perfect grid."
)

if len(network_cities) > 0:
    import matplotlib.pyplot as plt
    import matplotlib

    matplotlib.use("Agg")

    # Curated 9 cities: mix of grid, organic, and radial — skip lesser-known GHSL matches
    curated_names = [
        "Chicago", "Barcelona", "New York City",  # grid
        "Washington", "Singapore", "London",       # moderate
        "Tokyo", "Istanbul", "Paris",              # organic
    ]
    plot_cities = (
        network_cities[network_cities["city_name"].isin(curated_names)]
        .sort_values("orientation_order", ascending=False)
    )

    n_cities = len(plot_cities)
    n_cols = min(3, n_cities)
    n_rows = (n_cities + n_cols - 1) // n_cols

    fig, axes = plt.subplots(
        n_rows, n_cols,
        figsize=(4 * n_cols, 4 * n_rows),
        subplot_kw={"projection": "polar"},
    )
    fig.patch.set_facecolor("white")

    if n_rows == 1 and n_cols == 1:
        axes = np.array([axes])
    axes = np.atleast_2d(axes)

    # Color ramp: most ordered = vermillion, least = sky blue
    order_vals = plot_cities["orientation_order"].values
    order_min, order_max = order_vals.min(), order_vals.max()

    for idx, (_, row) in enumerate(plot_cities.iterrows()):
        r = idx // n_cols
        c = idx % n_cols
        ax = axes[r, c]

        # Parse bearing counts from JSON
        try:
            counts = json.loads(row["bearing_counts"])
        except (json.JSONDecodeError, TypeError):
            counts = [0] * 36

        n_bins = len(counts)
        bin_width = 2 * np.pi / n_bins
        angles = np.linspace(0, 2 * np.pi, n_bins, endpoint=False)

        # Normalize counts for consistent bar heights
        max_count = max(counts) if max(counts) > 0 else 1
        normalized = [c / max_count for c in counts]

        # Color based on orientation order
        t = ((row["orientation_order"] - order_min) / (order_max - order_min)
             if order_max > order_min else 0.5)
        # Interpolate vermillion (grid) to sky blue (organic)
        bar_color = (
            int(200 * t + 58 * (1 - t)) / 255,
            int(69 * t + 154 * (1 - t)) / 255,
            int(0 * t + 217 * (1 - t)) / 255,
        )

        ax.set_theta_zero_location("N")
        ax.set_theta_direction(-1)

        ax.bar(
            angles, normalized, width=bin_width,
            color=bar_color, alpha=0.85, edgecolor="white", linewidth=0.3,
        )

        ax.set_yticklabels([])
        ax.set_xticklabels([])
        ax.set_title(
            f"{row['city_name']}\nOrder: {row['orientation_order']:.2f}",
            fontsize=11, fontweight="bold", pad=12,
        )
        ax.grid(True, alpha=0.2)

        # Cardinal direction labels
        ax.set_xticks([0, np.pi / 2, np.pi, 3 * np.pi / 2])
        ax.set_xticklabels(["N", "E", "S", "W"], fontsize=8, color="#666")

    # Hide unused axes
    for idx in range(n_cities, n_rows * n_cols):
        r = idx // n_cols
        c = idx % n_cols
        axes[r, c].set_visible(False)

    plt.tight_layout(pad=2.0)
    st.pyplot(fig)
    plt.close(fig)

    # Insight blockquote
    most_ordered = plot_cities.iloc[0]
    least_ordered = plot_cities.iloc[-1]
    st.markdown(
        f"> **{most_ordered['city_name']}** has the most grid-like street network "
        f"(order = {most_ordered['orientation_order']:.2f}), while "
        f"**{least_ordered['city_name']}** is the most organic "
        f"(order = {least_ordered['orientation_order']:.2f}). "
        f"Grid cities channel traffic along cardinal directions; organic cities "
        f"distribute flow more evenly, often reflecting centuries of unplanned growth."
    )
else:
    st.info("No street network analysis data available. Run the street_networks raw asset first.")

st.caption(DATA_FOOTER, unsafe_allow_html=True)

st.markdown("---")

# ======================================================================
# CHART 3: Population vs. Grid Order
# ======================================================================

st.subheader("Does city size predict street grid order?")
st.caption(
    "Population (2015) vs. orientation order for the 19 cities with street network analysis. "
    "Each city is labeled. Dot size encodes intersection count; color encodes continent."
)

if len(network_cities) > 0:
    nc = network_cities.copy()

    sel5 = alt.selection_point(fields=["continent"], bind="legend")
    continent_order5 = sorted(nc["continent"].unique().tolist())
    colors5 = [CONTINENT_COLORS.get(c, MUTED) for c in continent_order5]

    scatter5 = (
        alt.Chart(nc)
        .mark_circle(strokeWidth=0.8, stroke="white")
        .encode(
            x=alt.X("population_2015:Q", title="Population (2015)",
                     scale=alt.Scale(type="log")),
            y=alt.Y("orientation_order:Q", title="Orientation Order (0=random, 1=grid)"),
            size=alt.Size("intersection_count:Q", title="Intersections",
                          scale=alt.Scale(range=[80, 800]),
                          legend=alt.Legend(orient="right")),
            color=alt.Color("continent:N", title="Continent",
                            scale=alt.Scale(domain=continent_order5, range=colors5)),
            opacity=alt.condition(sel5, alt.value(0.85), alt.value(0.15)),
            tooltip=[
                alt.Tooltip("city_name:N", title="City"),
                alt.Tooltip("country_name:N", title="Country"),
                alt.Tooltip("population_2015:Q", title="Population", format=",.0f"),
                alt.Tooltip("orientation_order:Q", title="Grid order", format=".3f"),
                alt.Tooltip("intersection_count:Q", title="Intersections", format=","),
            ],
        )
        .properties(height=450)
        .add_params(sel5)
    )

    labels5 = (
        alt.Chart(nc)
        .mark_text(dy=-12, fontSize=11, fontWeight="bold")
        .encode(
            x=alt.X("population_2015:Q", scale=alt.Scale(type="log")),
            y="orientation_order:Q",
            text="city_name:N",
            opacity=alt.condition(sel5, alt.value(1), alt.value(0)),
        )
    )

    st.altair_chart(scatter5 + labels5, use_container_width=True)

st.caption(DATA_FOOTER, unsafe_allow_html=True)
st.markdown("---")

# ======================================================================
# CHART 6: Building heights — global distribution + extremes
# ======================================================================

bh_data = cities[(cities["avg_building_height_m"].notna()) & (cities["population_2015"] >= 5_000_000)].copy()

st.subheader(f"Building heights across {len(bh_data):,} cities (population over 5M)")
st.caption(
    "Average building height from Sentinel-1 SAR data (GHS-BUILT-H). "
    "Each dot is a city with population over 5 million. X-axis: population. Y-axis: building height."
)

if len(bh_data) > 0:
    continent_order6 = sorted(bh_data["continent"].unique().tolist())
    colors6 = [CONTINENT_COLORS.get(c, MUTED) for c in continent_order6]
    sel6 = alt.selection_point(fields=["continent"], bind="legend")

    # Label all cities since the count is manageable at 5M+ threshold
    bh_data["bh_label"] = bh_data["city_name"]

    scatter6 = (
        alt.Chart(bh_data)
        .mark_circle(size=100, strokeWidth=0.8, stroke="white")
        .encode(
            x=alt.X("population_2015:Q", title="Population (2015)",
                     scale=alt.Scale(type="log", domain=[4500000, 45000000])),
            y=alt.Y("avg_building_height_m:Q", title="Avg building height (m)",
                     scale=alt.Scale(zero=False)),
            color=alt.Color("continent:N", title="Continent",
                            scale=alt.Scale(domain=continent_order6, range=colors6),
                            legend=alt.Legend(orient="top", direction="horizontal")),
            opacity=alt.condition(sel6, alt.value(0.8), alt.value(0.1)),
            tooltip=[
                alt.Tooltip("city_name:N", title="City"),
                alt.Tooltip("country_name:N", title="Country"),
                alt.Tooltip("avg_building_height_m:Q", title="Avg height (m)", format=".1f"),
                alt.Tooltip("population_2015:Q", title="Population", format=",.0f"),
                alt.Tooltip("hdi:Q", title="HDI", format=".2f"),
            ],
        )
        .properties(height=500)
        .add_params(sel6)
    )

    labels6 = (
        alt.Chart(bh_data)
        .mark_text(dy=-12, fontSize=12, fontWeight="bold")
        .encode(
            x=alt.X("population_2015:Q", scale=alt.Scale(type="log", domain=[4500000, 45000000])),
            y=alt.Y("avg_building_height_m:Q", scale=alt.Scale(zero=False)),
            text="bh_label:N",
            opacity=alt.condition(sel6, alt.value(1), alt.value(0)),
        )
    )

    st.altair_chart(scatter6 + labels6, use_container_width=True)

st.caption(DATA_FOOTER, unsafe_allow_html=True)
st.markdown("---")

# ======================================================================
# CHART 7: 20-city comparison — multi-metric radar-style bar chart
# ======================================================================

st.subheader("Comparing the 20 analyzed cities")
st.caption(
    "Six street network metrics side by side for all 20 cities with OSMnx analysis. "
    "Cities sorted by grid order (most grid-like at top). "
    "Each metric is normalized to 0-1 for comparison."
)

if len(network_cities) > 0:
    # Filter out lesser-known GHSL matches (Buenos Aires matched to nearby San Nicolás, Brasilia to Lago Norte)
    nc7 = (
        network_cities[~network_cities["city_name"].isin(["San Nicolás de los Arroyos", "Lago Norte"])]
        .copy()
        .sort_values("orientation_order", ascending=False)
    )

    # Compute intersection density (intersections per km²)
    nc7["intersection_density"] = nc7["intersection_count"] / nc7["area_km2"]

    # Normalize each metric to 0-1
    metrics_to_compare = {
        "pop_density_per_km2": "Pop density (ppl/km²)",
        "orientation_order": "Grid order (0-1)",
        "avg_building_height_m": "Building height (m)",
        "avg_street_length_m": "Street length (m)",
        "intersection_density": "Intersections (/km²)",
        "dead_end_proportion": "Dead ends (%)",
        "avg_circuity": "Route directness (ratio)",
    }

    melted_rows = []
    for col, label in metrics_to_compare.items():
        vals = nc7[col].astype(float)
        vmin, vmax = vals.min(), vals.max()
        rng = vmax - vmin if vmax != vmin else 1
        for _, row in nc7.iterrows():
            melted_rows.append({
                "city": row["city_name"],
                "metric": label,
                "raw_value": row[col],
                "normalized": (row[col] - vmin) / rng if pd.notna(row[col]) else 0,
            })

    melted = pd.DataFrame(melted_rows)

    # City order (most gridded first)
    city_order = nc7["city_name"].tolist()
    metric_order = list(metrics_to_compare.values())

    metric_colors = [VERMILLION, BLUE, BLUE_GREEN, ORANGE, PURPLE, SKY_BLUE]

    heatmap = (
        alt.Chart(melted)
        .mark_rect(cornerRadius=3)
        .encode(
            x=alt.X("metric:N", title=None, sort=metric_order,
                     axis=alt.Axis(labelAngle=0, labelFontSize=12)),
            y=alt.Y("city:N", title=None, sort=city_order,
                     axis=alt.Axis(labelFontSize=12)),
            color=alt.Color("normalized:Q", title="Relative value",
                            scale=alt.Scale(scheme="blues"),
                            legend=None),
            tooltip=[
                alt.Tooltip("city:N", title="City"),
                alt.Tooltip("metric:N", title="Metric"),
                alt.Tooltip("raw_value:Q", title="Value", format=".2f"),
            ],
        )
        .properties(height=500, width=600)
    )

    text7 = (
        alt.Chart(melted)
        .mark_text(fontSize=10)
        .encode(
            x=alt.X("metric:N", sort=metric_order),
            y=alt.Y("city:N", sort=city_order),
            text=alt.Text("raw_value:Q", format=".2f"),
            color=alt.condition(
                alt.datum.normalized > 0.6,
                alt.value("white"),
                alt.value("black"),
            ),
        )
    )

    st.altair_chart(heatmap + text7, use_container_width=True)

st.caption(DATA_FOOTER, unsafe_allow_html=True)
st.markdown("---")

# ======================================================================
# CHART 8: Climate vs. Urban Design
# ======================================================================

st.subheader("Climate and urban design")
st.caption(
    "How do climate conditions relate to street layout and building form? "
    "Each point is one of the 19 analyzed cities. "
    "Select a climate metric and an urban design metric to explore."
)

if len(network_cities) > 0:
    nc8 = network_cities[~network_cities["city_name"].isin(["San Nicolás de los Arroyos", "Lago Norte"])].copy()

    col_clim, col_urban = st.columns(2)
    with col_clim:
        x_metrics = {
            "Avg temperature (C)": "avg_temp_c",
            "Precipitation (mm/yr)": "precipitation_mm",
            "Elevation (m)": "elevation_m",
            "Population (2015)": "population_2015",
            "Pop density (ppl/km²)": "pop_density_per_km2",
            "HDI": "hdi",
            "Building height (m)": "avg_building_height_m",
            "Grid order (0-1)": "orientation_order",
            "Street length (m)": "avg_street_length_m",
            "Intersections": "intersection_count",
            "Dead-end proportion": "dead_end_proportion",
            "Route directness (ratio)": "avg_circuity",
        }
        clim_label = st.selectbox("X-axis:", list(x_metrics.keys()))
        clim_col = x_metrics[clim_label]

    with col_urban:
        y_metrics = {
            "Grid order (0-1)": "orientation_order",
            "Building height (m)": "avg_building_height_m",
            "Street length (m)": "avg_street_length_m",
            "Intersections": "intersection_count",
            "Dead-end proportion": "dead_end_proportion",
            "Route directness (ratio)": "avg_circuity",
            "Pop density (ppl/km²)": "pop_density_per_km2",
            "Avg temperature (C)": "avg_temp_c",
            "Precipitation (mm/yr)": "precipitation_mm",
            "Elevation (m)": "elevation_m",
            "Population (2015)": "population_2015",
            "HDI": "hdi",
        }
        urban_label = st.selectbox("Y-axis:", list(y_metrics.keys()))
        urban_col = y_metrics[urban_label]

    sel8 = alt.selection_point(fields=["continent"], bind="legend")
    continent_order8 = sorted(nc8["continent"].unique().tolist())
    colors8 = [CONTINENT_COLORS.get(c, MUTED) for c in continent_order8]

    scatter8 = (
        alt.Chart(nc8)
        .mark_circle(size=150, strokeWidth=1, stroke="white")
        .encode(
            x=alt.X(f"{clim_col}:Q", title=clim_label),
            y=alt.Y(f"{urban_col}:Q", title=urban_label),
            color=alt.Color("continent:N", title="Continent",
                            scale=alt.Scale(domain=continent_order8, range=colors8)),
            opacity=alt.condition(sel8, alt.value(0.85), alt.value(0.15)),
            tooltip=[
                alt.Tooltip("city_name:N", title="City"),
                alt.Tooltip("country_name:N", title="Country"),
                alt.Tooltip(f"{clim_col}:Q", title=clim_label, format=".1f"),
                alt.Tooltip(f"{urban_col}:Q", title=urban_label, format=".3f"),
                alt.Tooltip("population_2015:Q", title="Population", format=",.0f"),
            ],
        )
        .properties(height=450)
        .add_params(sel8)
    )

    labels8 = (
        alt.Chart(nc8)
        .mark_text(dy=-12, fontSize=11, fontWeight="bold")
        .encode(
            x=f"{clim_col}:Q",
            y=f"{urban_col}:Q",
            text="city_name:N",
            opacity=alt.condition(sel8, alt.value(1), alt.value(0)),
        )
    )

    st.altair_chart(scatter8 + labels8, use_container_width=True)

st.caption(DATA_FOOTER, unsafe_allow_html=True)

# ── Methodology ───────────────────────────────────────────────────────

st.markdown("---")
st.subheader("Methodology & Data Sources")
st.markdown("""
**Data sources:**
- [GHSL Urban Centre Database R2024A](https://ghsl.jrc.ec.europa.eu/) (EU JRC) -- pre-computed
  statistics for ~11,400 global urban centers including population at multiple epochs (1975-2030),
  GDP, building height, HDI, climate, and built-up area. CC BY 4.0.
- [OpenStreetMap](https://www.openstreetmap.org/) via [OSMnx](https://osmnx.readthedocs.io/) --
  street network graphs downloaded and analyzed for ~20 cities representing diverse urban
  planning traditions. ODbL license.
- [World Bank Open Data API](https://data.worldbank.org/) -- country-level urbanization
  indicators (6 indicators, 2000-2024). CC BY 4.0.

**Street network metrics:**
- **Orientation entropy:** Shannon entropy of the distribution of street bearings across 36 bins
  (10 degrees each). Lower entropy = more grid-like layout.
- **Orientation order:** 1 - (entropy / max_entropy). Ranges from 0 (random/organic) to 1
  (perfect grid). Max entropy = ln(36) ~ 3.58.
- **Dead-end proportion:** Fraction of network nodes with degree 1. Higher values indicate
  more cul-de-sacs and suburban-style development.
- **Circuity:** Average ratio of network route distance to straight-line distance. Values
  close to 1.0 mean direct routes; higher values mean winding streets.

**Definitions:**
- **Megacity:** Urban center with population over 10 million
- **Population tiers:** Megacity (10M+), Large (1-10M), Medium (100K-1M), Small (<100K)
- **Urbanization stage:** Rural (<30%), Transitioning (30-60%), Urban (60-80%), Hyper-urban (>80%)
- **Climate zone:** Simplified classification based on annual mean temperature and precipitation

**Limitations:**
- GHSL population estimates are for fixed epochs (1975, 1990, 2000, 2015), not annual
- GHSL GDP values are modeled estimates, not direct measurements
- Street network analysis covers ~20 cities only (Overpass API rate limits)
- OSMnx queries target central districts, not entire metropolitan areas
- World Bank data has ~2 year publication lag
""")

st.caption("Pipeline: Bruin + BigQuery  |  Analysis: OSMnx + NetworkX  |  Viz: Altair + Pydeck + Matplotlib")
