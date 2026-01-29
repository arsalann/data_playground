import os
from pathlib import Path
from typing import Optional

import altair as alt
import duckdb
import pandas as pd
import pydeck as pdk
import streamlit as st

try:
    import yaml
except ModuleNotFoundError:
    raise SystemExit(
        "Missing dependency 'pyyaml'. Install with: pip install pyyaml"
    )


st.set_page_config(page_title="NYC Taxi Insights Dashboard", layout="wide")

st.title("NYC Taxi Insights Dashboard")
st.caption("Built with Bruin + MotherDuck + Streamlit")

st.markdown(
    """
**About this dashboard**
- Tech: Bruin → MotherDuck (DuckDB) for data; Streamlit + Altair for viz.
- Data: NYC TLC yellow taxi trips (2020-01 to 2025-11).
- Summary: cleaned for distance/duration/speed/fare/tip sanity and borough validity; visuals cover tip% by time/flow and monthly totals. See footnotes for full criteria.
"""
)


def load_token() -> Optional[str]:
    # 1. Streamlit secrets (standard for Streamlit Cloud deployment)
    try:
        if "MOTHERDUCK_TOKEN" in st.secrets:
            return st.secrets["MOTHERDUCK_TOKEN"].strip().strip('"')
    except Exception:
        pass

    # 2. Environment variables
    env_token = os.getenv("MOTHERDUCK_TOKEN") or os.getenv("BRUIN_CONNECTION_MOTHERDUCK_PROD_TOKEN")
    if env_token:
        return env_token.strip().strip('"')

    # 3. Fallback: parse .bruin.yml for local development
    try:
        root = Path(__file__).resolve().parents[2]
        config_path = root / ".bruin.yml"
        if config_path.exists():
            with open(config_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
            token_val = (
                cfg.get("environments", {})
                .get("default", {})
                .get("connections", {})
                .get("motherduck", [{}])[0]
                .get("token")
            )
            return token_val.strip().strip('"') if token_val else None
    except Exception:
        return None
    return None


token = load_token()
if not token:
    st.error(
        "MotherDuck token missing. Add MOTHERDUCK_TOKEN to .streamlit/secrets.toml "
        "or set it as an environment variable."
    )
    st.stop()

# Ensure the extension also sees the token (some DuckDB setups read env)
os.environ["MOTHERDUCK_TOKEN"] = token


@st.cache_resource
def get_conn(md_token: str):
    # Connect via MotherDuck using DuckDB driver
    return duckdb.connect(f"md:?motherduck_token={md_token}")


base_path = Path(__file__).parent


@st.cache_data(show_spinner=False)
def run_query(filename: str) -> pd.DataFrame:
    sql = (base_path / filename).read_text()
    con = get_conn(token)
    return con.execute(sql).df()


# Load data (focused set)
socio = run_query("socio_trends.sql")
seasonality = run_query("seasonality_dow.sql")
dow_hour = run_query("dow_hour_tip.sql")
monthly_totals = run_query("monthly_totals.sql")
zone_tips_pickup = run_query("zone_tips_map.sql")
zone_tips_dropoff = run_query("zone_tips_map_dropoff.sql")
zone_payments = run_query("zone_payment_types.sql")
zone_zero_tip_cc = run_query("zone_zero_tip_cc.sql")
footnotes_text = (base_path / "footnotes.md").read_text(encoding="utf-8")

# Parse dates / derive
monthly_totals["month"] = pd.to_datetime(monthly_totals["month"])
if "tip_rate_pct" not in monthly_totals.columns:
    monthly_totals["tip_rate_pct"] = monthly_totals["tip_amount_sum"] / monthly_totals["fare_amount_sum"]
else:
    monthly_totals["tip_rate_pct"] = monthly_totals["tip_rate_pct"].fillna(
        monthly_totals["tip_amount_sum"] / monthly_totals["fare_amount_sum"]
    )

# Ensure numeric tip pct fields for charts
for df, col in [(seasonality, "avg_tip_pct"), (dow_hour, "avg_tip_pct"), (socio, "avg_tip_pct")]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

# Heatmap: day-of-week x month (tip rate)
st.subheader("Tip % by Day of Week and Month")
dow_labels = {0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat"}
dow_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
month_order = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
seasonality["dow_label"] = seasonality["dow"].map(dow_labels)
seasonality["month_label"] = seasonality["month_num"].apply(lambda m: pd.Timestamp(year=2024, month=int(m), day=1).strftime("%b"))

season_heat = (
    alt.Chart(seasonality)
    .mark_rect()
    .encode(
        x=alt.X("month_label:O", title="Month", sort=month_order),
        y=alt.Y("dow_label:O", title="Day of Week", sort=dow_order),
        color=alt.Color("avg_tip_pct:Q", title="Average Tip %", scale=alt.Scale(scheme="cividis")),
        tooltip=[
            alt.Tooltip("month_label", title="Month"),
            alt.Tooltip("dow_label", title="Day"),
            alt.Tooltip("avg_tip_pct", title="Average Tip %", format=".1%"),
            alt.Tooltip("zero_tip_rate", title="Zero-Tip Rate", format=".1%"),
            alt.Tooltip("med_fare_per_mile", title="Fare/mi", format=".2f"),
            alt.Tooltip("trips", title="Trips"),
        ],
    )
    .properties(height=280)
)

st.altair_chart(season_heat, use_container_width=True)

# Heatmap: day-of-week x hour (tip rate)
st.subheader("Tip % by Day of Week and Hour")
dow_hour["dow_label"] = dow_hour["dow"].map(dow_labels)
dow_hour_heat = (
    alt.Chart(dow_hour)
    .mark_rect()
    .encode(
        x=alt.X("pickup_hour:O", title="Hour of Day"),
        y=alt.Y("dow_label:O", title="Day of Week", sort=dow_order),
        color=alt.Color("avg_tip_pct:Q", title="Average Tip %", scale=alt.Scale(scheme="cividis")),
        tooltip=[
            alt.Tooltip("dow_label", title="Day"),
            alt.Tooltip("pickup_hour", title="Hour"),
            alt.Tooltip("avg_tip_pct", title="Average Tip %", format=".1%"),
            alt.Tooltip("zero_tip_rate", title="Zero-Tip Rate", format=".1%"),
            alt.Tooltip("med_fare_per_mile", title="Fare/mi", format=".2f"),
            alt.Tooltip("trips", title="Trips"),
        ],
    )
    .properties(height=300)
)
st.altair_chart(dow_hour_heat, use_container_width=True)

# Socioeconomic / human-interest signals
st.subheader("Borough-to-Borough Tip % (Average)")
latest_day = socio["daypart"].iloc[0] if not socio.empty else ""
socio_view = socio.copy()

heat_soc = (
    alt.Chart(socio_view)
    .mark_rect()
    .encode(
        x=alt.X("pickup_borough:N", title="Pickup Borough"),
        y=alt.Y("dropoff_borough:N", title="Dropoff Borough"),
        color=alt.Color("avg_tip_pct:Q", title="Average Tip %", scale=alt.Scale(scheme="cividis")),
        tooltip=[
            alt.Tooltip("pickup_borough", title="From"),
            alt.Tooltip("dropoff_borough", title="To"),
            alt.Tooltip("trips", title="Trips"),
            alt.Tooltip("avg_tip_pct", title="Average Tip %", format=".1%"),
            alt.Tooltip("zero_tip_rate", title="Zero-Tip Rate", format=".1%"),
            alt.Tooltip("med_fare_per_mile", title="Fare per Mile ($)", format=".2f"),
        ],
    )
    .properties(height=360)
)

bars_zero = (
    alt.Chart(socio_view.groupby("pickup_borough", as_index=False).agg({"zero_tip_rate": "mean", "trips": "sum"}))
    .mark_bar()
    .encode(
        x=alt.X("zero_tip_rate:Q", axis=alt.Axis(format="%"), title="Zero-Tip Rate"),
        y=alt.Y("pickup_borough:N", sort="-x", title="Pickup Borough"),
        color=alt.Color("trips:Q", title="Trips", scale=alt.Scale(scheme="cividis")),
        tooltip=[
            alt.Tooltip("pickup_borough", title="Borough"),
            alt.Tooltip("zero_tip_rate", title="Zero-Tip Rate", format=".1%"),
            alt.Tooltip("trips", title="Trips"),
        ],
    )
    .properties(height=360)
)

st.markdown("**Average tip % by borough flow**")
st.altair_chart(heat_soc, use_container_width=True)

# Payment Type Analysis by Zone
st.subheader("Cash vs Credit Card by Pickup Zone")
st.caption("Heatmap shows cash payment % by zone (brighter = more cash). Cash trips typically show $0 tip in the data.")

import numpy as np

# Prepare payment data for heatmap
zone_pay_map = zone_payments.copy()
zone_pay_map["cash_pct_display"] = (zone_pay_map["cash_pct"] * 100).round(1)
zone_pay_map["credit_card_pct_display"] = (zone_pay_map["credit_card_pct"] * 100).round(1)
zone_pay_map["weight"] = zone_pay_map["cash_pct"]  # Weight by cash percentage

heatmap_layer = pdk.Layer(
    "HeatmapLayer",
    data=zone_pay_map,
    get_position=["centroid_lon", "centroid_lat"],
    get_weight="weight",
    aggregation="MEAN",
    radius_pixels=35,
    intensity=0.8,
    threshold=0.05,
    opacity=0.75,
    # Colorblind-friendly: purple (low cash) → orange/yellow (high cash)
    color_range=[
        [63, 0, 125],     # Dark purple (low cash)
        [106, 81, 163],   # Purple
        [158, 154, 200],  # Light purple
        [253, 184, 99],   # Orange
        [254, 224, 139],  # Light orange
        [255, 255, 191],  # Yellow (high cash)
    ],
)

view_state = pdk.ViewState(
    latitude=40.75,
    longitude=-73.95,
    zoom=10,
    pitch=0,
)

st.pydeck_chart(pdk.Deck(
    layers=[heatmap_layer],
    initial_view_state=view_state,
    map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
))

# Summary stats
st.markdown("**Payment Type Summary by Borough**")
borough_payments = zone_payments.groupby("borough").agg({
    "total_trips": "sum",
    "credit_card_trips": "sum",
    "cash_trips": "sum",
}).reset_index()
borough_payments["credit_card_pct"] = (borough_payments["credit_card_trips"] / borough_payments["total_trips"] * 100).round(1)
borough_payments["cash_pct"] = (borough_payments["cash_trips"] / borough_payments["total_trips"] * 100).round(1)

borough_chart = (
    alt.Chart(borough_payments)
    .transform_fold(["credit_card_pct", "cash_pct"], as_=["Payment Type", "Percentage"])
    .mark_bar()
    .encode(
        x=alt.X("borough:N", title="Borough", sort="-y"),
        y=alt.Y("Percentage:Q", title="% of Trips"),
        color=alt.Color(
            "Payment Type:N",
            scale=alt.Scale(domain=["credit_card_pct", "cash_pct"], range=["#4575b4", "#fdae61"]),
            legend=alt.Legend(title="Payment", labelExpr="datum.value === 'credit_card_pct' ? 'Credit Card' : 'Cash'"),
        ),
        xOffset="Payment Type:N",
        tooltip=[
            alt.Tooltip("borough", title="Borough"),
            alt.Tooltip("Payment Type:N", title="Type"),
            alt.Tooltip("Percentage:Q", title="%", format=".1f"),
            alt.Tooltip("total_trips:Q", title="Total Trips", format=","),
        ],
    )
    .properties(height=280)
)
st.altair_chart(borough_chart, use_container_width=True)

# Low Tip Rate for Credit Card Payments Map
st.subheader("Low-Tip Rate for Credit Card Payments")

# Slider for tip threshold
tip_threshold = st.slider(
    "Show trips with tip % below:",
    min_value=0,
    max_value=25,
    value=0,
    step=5,
    format="%d%%",
)

# Map threshold to column
threshold_map = {
    0: "tips_0pct",
    5: "tips_under_5pct",
    10: "tips_under_10pct",
    15: "tips_under_15pct",
    20: "tips_under_20pct",
    25: "tips_under_25pct",
}
threshold_col = threshold_map.get(tip_threshold, "tips_0pct")

# Caption based on threshold
if tip_threshold == 0:
    st.caption("Where do people skip tipping despite using a card? (brighter = more $0 tips)")
else:
    st.caption(f"Where do people tip less than {tip_threshold}%? (brighter = more low tippers)")

zone_zero_map = zone_zero_tip_cc.copy()

# Handle old cached data that might not have new columns
if threshold_col not in zone_zero_map.columns:
    # Fallback: use zero_tip_pct if available, otherwise recalculate
    if "zero_tip_pct" in zone_zero_map.columns:
        zone_zero_map["threshold_count"] = zone_zero_map["zero_tip_pct"] * zone_zero_map["cc_trips"]
    else:
        st.warning("Please restart the Streamlit app to load updated data with threshold columns.")
        st.stop()
else:
    zone_zero_map["threshold_count"] = zone_zero_map[threshold_col]
zone_zero_map["threshold_pct"] = zone_zero_map["threshold_count"] / zone_zero_map["cc_trips"]
zone_zero_map["threshold_pct_display"] = (zone_zero_map["threshold_pct"] * 100).round(1)
zone_zero_map["weight"] = zone_zero_map["threshold_pct"]

zero_tip_heatmap = pdk.Layer(
    "HeatmapLayer",
    data=zone_zero_map,
    get_position=["centroid_lon", "centroid_lat"],
    get_weight="weight",
    aggregation="MEAN",
    radius_pixels=35,
    intensity=1.0,
    threshold=0.05,
    opacity=0.75,
    # Colorblind-friendly: dark blue (low) → orange/red (high)
    color_range=[
        [49, 54, 149],    # Dark blue (low - people tip well)
        [69, 117, 180],   # Blue
        [116, 173, 209],  # Light blue
        [254, 224, 144],  # Light yellow
        [253, 174, 97],   # Orange
        [244, 109, 67],   # Red-orange (high - people tip poorly)
    ],
)

view_state_zero = pdk.ViewState(
    latitude=40.75,
    longitude=-73.95,
    zoom=10,
    pitch=0,
)

st.pydeck_chart(pdk.Deck(
    layers=[zero_tip_heatmap],
    initial_view_state=view_state_zero,
    map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
))

# Cleaning log (from footnotes)
with st.expander("Data cleaning applied (from footnotes)"):
    st.markdown(footnotes_text)

# Basic monthly totals (filtered) – placed at bottom
st.subheader("Monthly Totals (Filtered)")

totals_chart = (
    alt.Chart(monthly_totals)
    .mark_bar(color="#1f78b4")
    .encode(
        x=alt.X("month:T", title="Month"),
        y=alt.Y("total_amount_sum:Q", title="Total Amount ($)"),
        tooltip=[
            alt.Tooltip("month:T", title="Month"),
            alt.Tooltip("trips:Q", title="Trips", format=","),
            alt.Tooltip("fare_amount_sum:Q", title="Fare Sum", format=",.0f"),
            alt.Tooltip("tip_amount_sum:Q", title="Tip Sum", format=",.0f"),
            alt.Tooltip("total_amount_sum:Q", title="Total Sum", format=",.0f"),
        ],
    )
    .properties(height=320)
)
tip_line_totals = (
    alt.Chart(monthly_totals)
    .mark_line(color="#33a02c", strokeWidth=3)
    .encode(
        x="month:T",
        y=alt.Y("tip_rate_pct:Q", axis=alt.Axis(format="%"), title="Tip Rate %"),
        tooltip=[
            alt.Tooltip("month:T", title="Month"),
            alt.Tooltip("tip_rate_pct:Q", title="Tip Rate %", format=".1%"),
        ],
    )
)
st.altair_chart(
    alt.layer(totals_chart, tip_line_totals).resolve_scale(y="independent"),
    use_container_width=True,
)
