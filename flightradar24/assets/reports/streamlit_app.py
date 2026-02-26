from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(
    page_title="Who Rules the Skies?",
    layout="wide",
)

PROJECT_ID = "bruin-playground-arsalan"
base_path = Path(__file__).parent

HIGHLIGHT = "#D55E00"
DEFAULT = "#56B4E9"

REGION_COLORS = {
    "Middle East": "#D55E00",
    "North America": "#56B4E9",
    "Europe": "#E69F00",
    "East Asia": "#009E73",
    "Southeast Asia": "#0072B2",
    "South Asia": "#CC79A7",
    "Oceania": "#F0E442",
    "South America": "#999999",
    "Africa": "#882255",
    "Other": "#BBBBBB",
}


@st.cache_resource
def get_client():
    credentials = service_account.Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)


def run_raw(sql: str) -> pd.DataFrame:
    return get_client().query(sql).to_dataframe()


def run_query(filename: str) -> pd.DataFrame:
    sql = (base_path / filename).read_text()
    return get_client().query(sql).to_dataframe()


# ──────────────────────────────────────────────────────────────────────
# Load data
# ──────────────────────────────────────────────────────────────────────

hub = run_query("hub_comparison.sql")
flights = run_raw(
    "SELECT * FROM `bruin-playground-arsalan.staging.flights_by_hub`"
)

# ──────────────────────────────────────────────────────────────────────
# Header
# ──────────────────────────────────────────────────────────────────────

st.title("Who Rules the Skies?")
st.caption(
    "Comparing the world's busiest airport mega-hubs in 2026  ·  "
    "Data: Flightradar24 API  ·  Built with Bruin + BigQuery + Streamlit"
)

if flights.empty:
    st.warning("No flight data loaded yet. Run the Bruin pipeline to ingest data.")
    st.stop()

total_flights = len(flights)
airports_queried = flights["query_airport"].nunique()
airlines_seen = flights["airline_icao"].nunique()

col1, col2, col3 = st.columns(3)
col1.metric("Total Flights", f"{total_flights:,}")
col2.metric("Airports Compared", airports_queried)
col3.metric("Unique Airlines", airlines_seen)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════
# 1. Flights per Hub
# ══════════════════════════════════════════════════════════════════════

st.subheader("Flights per Hub")
st.caption(
    "Total sampled flights by airport. Dubai (DXB) overtook Atlanta (ATL) as the "
    "world's busiest airport in early 2026, powered by its position as a global "
    "connecting hub between Europe, Asia, and Africa."
)

hub_sorted = hub.sort_values("total_flights", ascending=False)
region_domain = list(REGION_COLORS.keys())
region_range = list(REGION_COLORS.values())

flights_chart = (
    alt.Chart(hub_sorted)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    .encode(
        x=alt.X(
            "query_airport:N",
            title="Airport",
            sort=list(hub_sorted["query_airport"]),
        ),
        y=alt.Y("total_flights:Q", title="Flights"),
        color=alt.Color(
            "query_airport_region:N",
            title="Region",
            scale=alt.Scale(domain=region_domain, range=region_range),
        ),
        tooltip=[
            alt.Tooltip("query_airport:N", title="Airport"),
            alt.Tooltip("query_airport_region:N", title="Region"),
            alt.Tooltip("total_flights:Q", title="Flights"),
            alt.Tooltip("unique_airlines:Q", title="Airlines"),
        ],
    )
    .properties(height=340)
)

st.altair_chart(flights_chart, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════
# 2. Hub Reach: Average Flight Distance
# ══════════════════════════════════════════════════════════════════════

st.subheader("Hub Reach: Average Flight Distance")
st.caption(
    "Average actual distance per flight in kilometers. Middle Eastern hubs like DXB "
    "serve as intercontinental connectors with longer average distances, while domestic-heavy "
    "hubs like ATL skew shorter."
)

reach_chart = (
    alt.Chart(hub_sorted)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    .encode(
        x=alt.X(
            "query_airport:N",
            title="Airport",
            sort=list(hub_sorted["query_airport"]),
        ),
        y=alt.Y("avg_distance_km:Q", title="Avg Distance (km)"),
        color=alt.Color(
            "query_airport_region:N",
            title="Region",
            scale=alt.Scale(domain=region_domain, range=region_range),
        ),
        tooltip=[
            alt.Tooltip("query_airport:N", title="Airport"),
            alt.Tooltip("avg_distance_km:Q", title="Avg Distance (km)", format=",.0f"),
            alt.Tooltip("max_distance_km:Q", title="Max Distance (km)", format=",.0f"),
            alt.Tooltip("avg_duration_min:Q", title="Avg Duration (min)", format=".0f"),
        ],
    )
    .properties(height=340)
)

overall_avg = hub["avg_distance_km"].mean()
avg_line = (
    alt.Chart(pd.DataFrame({"avg": [overall_avg]}))
    .mark_rule(color="#999999", strokeDash=[6, 3], strokeWidth=2)
    .encode(y="avg:Q")
)
avg_label = (
    alt.Chart(
        pd.DataFrame({"avg": [overall_avg], "label": [f"Overall avg: {overall_avg:,.0f} km"]})
    )
    .mark_text(align="left", dx=5, dy=-8, color="#999999", fontSize=12)
    .encode(y="avg:Q", text="label:N")
)

st.altair_chart(reach_chart + avg_line + avg_label, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════
# 3. Fleet Mix: Airbus vs Boeing
# ══════════════════════════════════════════════════════════════════════

st.subheader("Fleet Mix: Airbus vs Boeing vs Other")
st.caption(
    "Manufacturer breakdown of aircraft serving each hub. The Airbus/Boeing duopoly "
    "dominates, but regional jets (Embraer, Bombardier) play a larger role at "
    "domestic-heavy airports."
)

fleet_data = hub_sorted[["query_airport", "airbus_flights", "boeing_flights", "other_manufacturer_flights"]].melt(
    id_vars=["query_airport"],
    value_vars=["airbus_flights", "boeing_flights", "other_manufacturer_flights"],
    var_name="manufacturer_raw",
    value_name="flights",
)
fleet_data["manufacturer"] = fleet_data["manufacturer_raw"].map({
    "airbus_flights": "Airbus",
    "boeing_flights": "Boeing",
    "other_manufacturer_flights": "Other",
})

fleet_chart = (
    alt.Chart(fleet_data)
    .mark_bar()
    .encode(
        x=alt.X(
            "query_airport:N",
            title="Airport",
            sort=list(hub_sorted["query_airport"]),
        ),
        y=alt.Y("flights:Q", title="Flights", stack="normalize", axis=alt.Axis(format="%")),
        color=alt.Color(
            "manufacturer:N",
            title="Manufacturer",
            scale=alt.Scale(
                domain=["Airbus", "Boeing", "Other"],
                range=["#0072B2", "#D55E00", "#999999"],
            ),
        ),
        tooltip=[
            alt.Tooltip("query_airport:N", title="Airport"),
            alt.Tooltip("manufacturer:N", title="Manufacturer"),
            alt.Tooltip("flights:Q", title="Flights"),
        ],
    )
    .properties(height=340)
)

st.altair_chart(fleet_chart, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════
# 4. Airline Diversity
# ══════════════════════════════════════════════════════════════════════

st.subheader("Airline Diversity: How Open Is Each Hub?")
st.caption(
    "Number of unique airlines operating at each hub. A higher count indicates a more "
    "internationally connected airport with diverse carrier access."
)

hub_sorted["is_top"] = hub_sorted["unique_airlines"] == hub_sorted["unique_airlines"].max()

airline_chart = (
    alt.Chart(hub_sorted)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    .encode(
        x=alt.X(
            "query_airport:N",
            title="Airport",
            sort=list(hub_sorted["query_airport"]),
        ),
        y=alt.Y("unique_airlines:Q", title="Unique Airlines"),
        color=alt.condition(
            alt.datum.is_top,
            alt.value(HIGHLIGHT),
            alt.value(DEFAULT),
        ),
        tooltip=[
            alt.Tooltip("query_airport:N", title="Airport"),
            alt.Tooltip("unique_airlines:Q", title="Unique Airlines"),
            alt.Tooltip("total_flights:Q", title="Total Flights"),
        ],
    )
    .properties(height=340)
)

st.altair_chart(airline_chart, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════
# 5. Passenger vs Cargo
# ══════════════════════════════════════════════════════════════════════

st.subheader("Passenger vs Cargo Split")
st.caption(
    "Proportion of passenger versus cargo flights at each hub. Airports like DXB "
    "and HKG historically have a significant cargo presence alongside passenger traffic."
)

category_data = hub_sorted[["query_airport", "passenger_flights", "cargo_flights", "other_category_flights"]].melt(
    id_vars=["query_airport"],
    value_vars=["passenger_flights", "cargo_flights", "other_category_flights"],
    var_name="category_raw",
    value_name="flights",
)
category_data["category"] = category_data["category_raw"].map({
    "passenger_flights": "Passenger",
    "cargo_flights": "Cargo",
    "other_category_flights": "Other",
})

category_chart = (
    alt.Chart(category_data)
    .mark_bar()
    .encode(
        x=alt.X(
            "query_airport:N",
            title="Airport",
            sort=list(hub_sorted["query_airport"]),
        ),
        y=alt.Y("flights:Q", title="Flights", stack="normalize", axis=alt.Axis(format="%")),
        color=alt.Color(
            "category:N",
            title="Category",
            scale=alt.Scale(
                domain=["Passenger", "Cargo", "Other"],
                range=["#56B4E9", "#E69F00", "#999999"],
            ),
        ),
        tooltip=[
            alt.Tooltip("query_airport:N", title="Airport"),
            alt.Tooltip("category:N", title="Category"),
            alt.Tooltip("flights:Q", title="Flights"),
        ],
    )
    .properties(height=340)
)

st.altair_chart(category_chart, use_container_width=True)

# ──────────────────────────────────────────────────────────────────────
# Distance breakdown detail
# ──────────────────────────────────────────────────────────────────────

st.markdown("---")
st.subheader("Route Length Profile")
st.caption(
    "Share of short-haul (<1,500 km), medium-haul (1,500-4,000 km), and long-haul "
    "(>4,000 km) flights. Intercontinental connectors like DXB and LHR lean long-haul, "
    "while domestic giants like ATL are short-haul dominant."
)

distance_data = hub_sorted[["query_airport", "short_haul", "medium_haul", "long_haul"]].melt(
    id_vars=["query_airport"],
    value_vars=["short_haul", "medium_haul", "long_haul"],
    var_name="distance_raw",
    value_name="flights",
)
distance_data["distance_category"] = distance_data["distance_raw"].map({
    "short_haul": "Short-haul",
    "medium_haul": "Medium-haul",
    "long_haul": "Long-haul",
})

distance_chart = (
    alt.Chart(distance_data)
    .mark_bar()
    .encode(
        x=alt.X(
            "query_airport:N",
            title="Airport",
            sort=list(hub_sorted["query_airport"]),
        ),
        y=alt.Y("flights:Q", title="Flights", stack="normalize", axis=alt.Axis(format="%")),
        color=alt.Color(
            "distance_category:N",
            title="Distance",
            scale=alt.Scale(
                domain=["Short-haul", "Medium-haul", "Long-haul"],
                range=["#56B4E9", "#E69F00", "#D55E00"],
            ),
            sort=["Short-haul", "Medium-haul", "Long-haul"],
        ),
        tooltip=[
            alt.Tooltip("query_airport:N", title="Airport"),
            alt.Tooltip("distance_category:N", title="Distance"),
            alt.Tooltip("flights:Q", title="Flights"),
        ],
    )
    .properties(height=340)
)

st.altair_chart(distance_chart, use_container_width=True)

# ──────────────────────────────────────────────────────────────────────
# Footer
# ──────────────────────────────────────────────────────────────────────

st.markdown("---")
st.caption(
    "Data: Flightradar24 API (Flight Summary Full)  ·  "
    "Pipeline: Bruin  ·  Database: BigQuery  ·  Visualization: Streamlit + Altair"
)
