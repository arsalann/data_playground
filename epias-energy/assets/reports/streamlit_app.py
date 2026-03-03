from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(
    page_title="Turkey Energy Market Dashboard",
    layout="wide",
)

PROJECT_ID = "bruin-playground-arsalan"
base_path = Path(__file__).parent

HIGHLIGHT = "#D55E00"
DEFAULT = "#56B4E9"
SECONDARY = "#E69F00"
MUTED = "#999999"

SOURCE_COLORS = {
    "natural_gas": "#E69F00",
    "wind": "#56B4E9",
    "solar": "#F0E442",
    "lignite": "#8B4513",
    "hard_coal": "#333333",
    "dammed_hydro": "#0072B2",
    "river": "#009E73",
    "geothermal": "#CC79A7",
    "biomass": "#66AA55",
    "fuel_oil": "#882255",
    "naphta": "#AA4499",
    "import_export": "#999999",
}

SOURCE_LABELS = {
    "natural_gas": "Natural Gas",
    "wind": "Wind",
    "solar": "Solar",
    "lignite": "Lignite",
    "hard_coal": "Hard Coal",
    "dammed_hydro": "Dammed Hydro",
    "river": "Run-of-River",
    "geothermal": "Geothermal",
    "biomass": "Biomass",
    "fuel_oil": "Fuel Oil",
    "naphta": "Naphta",
    "import_export": "Import/Export",
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


@st.cache_data(ttl=3600)
def load_generation():
    return run_raw(
        "SELECT * FROM `bruin-playground-arsalan.staging.epias_generation_daily` ORDER BY date"
    )


@st.cache_data(ttl=3600)
def load_forecast():
    return run_raw(
        "SELECT * FROM `bruin-playground-arsalan.staging.epias_forecast_vs_actual` ORDER BY date"
    )


@st.cache_data(ttl=3600)
def load_prices():
    return run_raw(
        "SELECT * FROM `bruin-playground-arsalan.staging.epias_market_prices_daily` ORDER BY date"
    )


st.title("Turkey Energy Market Dashboard")
st.caption(
    "EPIAS Transparency Platform data  ·  "
    "Built with Bruin + BigQuery + Streamlit"
)

tab_gen, tab_forecast, tab_prices = st.tabs([
    "Generation by Source",
    "Forecast vs Actual",
    "Market Prices",
])

# ======================================================================
# Tab 1: Generation by Source
# ======================================================================

with tab_gen:
    gen = load_generation()
    gen["date"] = pd.to_datetime(gen["date"])
    gen_sources = gen[gen["source_name"] != "import_export"].copy()
    gen_sources["source_label"] = gen_sources["source_name"].map(SOURCE_LABELS)

    st.subheader("Energy Generation by Source")
    st.caption("Daily electricity generation in Turkey, broken down by energy source (MWh).")

    total_by_source = (
        gen_sources.groupby("source_name")["generation_mwh"]
        .sum()
        .sort_values(ascending=False)
    )
    top_sources = total_by_source.head(6).index.tolist()

    col1, col2, col3 = st.columns(3)
    total_gen = total_by_source.sum()
    with col1:
        st.metric("Total Generation", f"{total_gen / 1e6:,.1f} TWh")
    with col2:
        top_source = total_by_source.index[0]
        top_pct = total_by_source.iloc[0] / total_gen * 100
        st.metric("Top Source", SOURCE_LABELS.get(top_source, top_source), f"{top_pct:.1f}%")
    with col3:
        renewables = total_by_source[
            total_by_source.index.isin(["wind", "solar", "geothermal", "biomass", "river", "dammed_hydro"])
        ].sum()
        ren_pct = renewables / total_gen * 100
        st.metric("Renewable Share", f"{ren_pct:.1f}%")

    st.markdown("---")

    monthly_source = (
        gen_sources.assign(month=gen_sources["date"].dt.to_period("M").dt.to_timestamp())
        .groupby(["month", "source_name", "source_label"])["generation_mwh"]
        .sum()
        .reset_index()
    )

    source_order = total_by_source.index.tolist()
    source_labels_ordered = [SOURCE_LABELS.get(s, s) for s in source_order]
    source_color_values = [SOURCE_COLORS.get(s, MUTED) for s in source_order]

    stacked_area = (
        alt.Chart(monthly_source)
        .mark_area()
        .encode(
            x=alt.X("month:T", title="Month"),
            y=alt.Y("generation_mwh:Q", title="Generation (MWh)", stack="zero"),
            color=alt.Color(
                "source_label:N",
                title="Source",
                scale=alt.Scale(domain=source_labels_ordered, range=source_color_values),
                sort=source_labels_ordered,
            ),
            tooltip=[
                alt.Tooltip("month:T", title="Month", format="%b %Y"),
                alt.Tooltip("source_label:N", title="Source"),
                alt.Tooltip("generation_mwh:Q", title="Generation (MWh)", format=",.0f"),
            ],
        )
        .properties(height=400)
    )
    st.altair_chart(stacked_area, use_container_width=True)

    st.markdown("#### Source Mix Breakdown")
    pie_data = total_by_source.reset_index()
    pie_data.columns = ["source_name", "total_mwh"]
    pie_data["source_label"] = pie_data["source_name"].map(SOURCE_LABELS)
    pie_data["pct"] = (pie_data["total_mwh"] / pie_data["total_mwh"].sum() * 100).round(1)

    pie_chart = (
        alt.Chart(pie_data)
        .mark_arc(innerRadius=50, outerRadius=140)
        .encode(
            theta=alt.Theta("total_mwh:Q"),
            color=alt.Color(
                "source_label:N",
                title="Source",
                scale=alt.Scale(domain=source_labels_ordered, range=source_color_values),
                sort=source_labels_ordered,
            ),
            tooltip=[
                alt.Tooltip("source_label:N", title="Source"),
                alt.Tooltip("total_mwh:Q", title="Total (MWh)", format=",.0f"),
                alt.Tooltip("pct:Q", title="Share (%)", format=".1f"),
            ],
        )
        .properties(height=340, width=340)
    )
    st.altair_chart(pie_chart, use_container_width=False)


# ======================================================================
# Tab 2: Forecast vs Actual
# ======================================================================

with tab_forecast:
    fva = load_forecast()
    fva["date"] = pd.to_datetime(fva["date"])

    st.subheader("Day-Ahead Forecast vs Actual Generation")
    st.caption(
        "Compares the day-ahead production plan (DPP first version) against "
        "actual real-time generation. Positive error = over-forecast."
    )

    fva_total = fva[fva["source_name"] == "total"].copy()

    if len(fva_total):
        avg_error = fva_total["error_pct"].mean()
        avg_abs_error = fva_total["abs_error_mwh"].mean()
        mae_pct = fva_total["error_pct"].abs().mean()

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Mean Error (%)", f"{avg_error:+.2f}%")
        with col2:
            st.metric("Mean Abs Error", f"{avg_abs_error:,.0f} MWh")
        with col3:
            st.metric("MAPE", f"{mae_pct:.2f}%")

        st.markdown("---")

        monthly_fva = (
            fva_total
            .assign(month=fva_total["date"].dt.to_period("M").dt.to_timestamp())
            .groupby("month")
            .agg(
                forecast_mwh=("forecast_mwh", "sum"),
                actual_mwh=("actual_mwh", "sum"),
            )
            .reset_index()
        )
        monthly_fva_long = monthly_fva.melt(
            id_vars="month",
            value_vars=["forecast_mwh", "actual_mwh"],
            var_name="type",
            value_name="mwh",
        )
        monthly_fva_long["type"] = monthly_fva_long["type"].map({
            "forecast_mwh": "Forecast (DPP)",
            "actual_mwh": "Actual",
        })

        fva_lines = (
            alt.Chart(monthly_fva_long)
            .mark_line(strokeWidth=2.5)
            .encode(
                x=alt.X("month:T", title="Month"),
                y=alt.Y("mwh:Q", title="Total Generation (MWh)"),
                color=alt.Color(
                    "type:N",
                    title="Type",
                    scale=alt.Scale(
                        domain=["Forecast (DPP)", "Actual"],
                        range=[SECONDARY, DEFAULT],
                    ),
                ),
                tooltip=[
                    alt.Tooltip("month:T", title="Month", format="%b %Y"),
                    alt.Tooltip("type:N", title="Type"),
                    alt.Tooltip("mwh:Q", title="Generation (MWh)", format=",.0f"),
                ],
            )
            .properties(height=380)
        )
        st.altair_chart(fva_lines, use_container_width=True)

        st.markdown("#### Forecast Error by Source")
        st.caption("Average daily forecast error (%) for each energy source.")

        fva_by_source = (
            fva[fva["source_name"] != "total"]
            .groupby("source_name")["error_pct"]
            .mean()
            .reset_index()
        )
        fva_by_source["source_label"] = fva_by_source["source_name"].map(SOURCE_LABELS)
        fva_by_source = fva_by_source.sort_values("error_pct")

        error_bars = (
            alt.Chart(fva_by_source)
            .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
            .encode(
                x=alt.X("error_pct:Q", title="Avg Forecast Error (%)"),
                y=alt.Y(
                    "source_label:N",
                    title=None,
                    sort=alt.EncodingSortField(field="error_pct", order="ascending"),
                ),
                color=alt.condition(
                    alt.datum.error_pct > 0,
                    alt.value(HIGHLIGHT),
                    alt.value(DEFAULT),
                ),
                tooltip=[
                    alt.Tooltip("source_label:N", title="Source"),
                    alt.Tooltip("error_pct:Q", title="Avg Error (%)", format="+.2f"),
                ],
            )
            .properties(height=340)
        )

        zero_line = (
            alt.Chart(pd.DataFrame({"x": [0]}))
            .mark_rule(color="#333333", strokeWidth=1)
            .encode(x="x:Q")
        )
        st.altair_chart(error_bars + zero_line, use_container_width=True)
    else:
        st.warning("No total forecast data available.")


# ======================================================================
# Tab 3: Market Prices
# ======================================================================

with tab_prices:
    prices = load_prices()
    prices["date"] = pd.to_datetime(prices["date"])

    st.subheader("Electricity Market Prices")
    st.caption(
        "Day-Ahead Market Clearing Price (MCP) and Balancing Market "
        "System Marginal Price (SMP) daily averages."
    )

    if len(prices):
        avg_mcp = prices["mcp_avg"].mean()
        avg_smp = prices["smp_avg"].mean()
        avg_spread = prices["spread_avg"].mean()

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Avg MCP", f"{avg_mcp:,.1f} TRY/MWh")
        with col2:
            st.metric("Avg SMP", f"{avg_smp:,.1f} TRY/MWh")
        with col3:
            st.metric("Avg Spread", f"{avg_spread:+,.1f} TRY/MWh")

        st.markdown("---")

        price_long = prices.melt(
            id_vars="date",
            value_vars=["mcp_avg", "smp_avg"],
            var_name="price_type",
            value_name="try_mwh",
        )
        price_long["price_type"] = price_long["price_type"].map({
            "mcp_avg": "MCP (Day-Ahead)",
            "smp_avg": "SMP (Balancing)",
        })

        price_lines = (
            alt.Chart(price_long)
            .mark_line(strokeWidth=1.5)
            .encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("try_mwh:Q", title="Price (TRY/MWh)"),
                color=alt.Color(
                    "price_type:N",
                    title="Price Type",
                    scale=alt.Scale(
                        domain=["MCP (Day-Ahead)", "SMP (Balancing)"],
                        range=[DEFAULT, HIGHLIGHT],
                    ),
                ),
                tooltip=[
                    alt.Tooltip("date:T", title="Date", format="%b %d, %Y"),
                    alt.Tooltip("price_type:N", title="Type"),
                    alt.Tooltip("try_mwh:Q", title="Price (TRY/MWh)", format=",.1f"),
                ],
            )
            .properties(height=380)
        )
        st.altair_chart(price_lines, use_container_width=True)

        st.markdown("#### Price Spread (SMP - MCP)")
        st.caption(
            "The spread between balancing market price and day-ahead price. "
            "Positive = SMP above MCP (real-time costs exceeded forecasts)."
        )

        spread_bars = (
            alt.Chart(prices)
            .mark_bar()
            .encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("spread_avg:Q", title="Spread (TRY/MWh)"),
                color=alt.condition(
                    alt.datum.spread_avg > 0,
                    alt.value(HIGHLIGHT),
                    alt.value(DEFAULT),
                ),
                tooltip=[
                    alt.Tooltip("date:T", title="Date", format="%b %d, %Y"),
                    alt.Tooltip("spread_avg:Q", title="Spread (TRY/MWh)", format="+,.1f"),
                    alt.Tooltip("mcp_avg:Q", title="MCP Avg", format=",.1f"),
                    alt.Tooltip("smp_avg:Q", title="SMP Avg", format=",.1f"),
                ],
            )
            .properties(height=340)
        )

        zero_line = (
            alt.Chart(pd.DataFrame({"y": [0]}))
            .mark_rule(color="#333333", strokeWidth=1)
            .encode(y="y:Q")
        )
        st.altair_chart(spread_bars + zero_line, use_container_width=True)

        st.markdown("#### Monthly Price Summary")
        monthly_prices = (
            prices
            .assign(month=prices["date"].dt.to_period("M").dt.to_timestamp())
            .groupby("month")
            .agg(
                mcp_avg=("mcp_avg", "mean"),
                smp_avg=("smp_avg", "mean"),
                mcp_min=("mcp_min", "min"),
                mcp_max=("mcp_max", "max"),
                surplus_hours=("surplus_hours", "sum"),
                deficit_hours=("deficit_hours", "sum"),
            )
            .reset_index()
        )

        monthly_price_long = monthly_prices.melt(
            id_vars="month",
            value_vars=["mcp_avg", "smp_avg"],
            var_name="type",
            value_name="avg_price",
        )
        monthly_price_long["type"] = monthly_price_long["type"].map({
            "mcp_avg": "MCP",
            "smp_avg": "SMP",
        })

        monthly_bar = (
            alt.Chart(monthly_price_long)
            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X("month:T", title="Month"),
                y=alt.Y("avg_price:Q", title="Avg Price (TRY/MWh)"),
                color=alt.Color(
                    "type:N",
                    title="Price Type",
                    scale=alt.Scale(
                        domain=["MCP", "SMP"],
                        range=[DEFAULT, HIGHLIGHT],
                    ),
                ),
                xOffset="type:N",
                tooltip=[
                    alt.Tooltip("month:T", title="Month", format="%b %Y"),
                    alt.Tooltip("type:N", title="Type"),
                    alt.Tooltip("avg_price:Q", title="Avg Price", format=",.1f"),
                ],
            )
            .properties(height=340)
        )
        st.altair_chart(monthly_bar, use_container_width=True)
    else:
        st.warning("No price data available.")


# ======================================================================
# Footer
# ======================================================================

st.markdown("---")
st.caption(
    "Data: EPIAS Transparency Platform (seffaflik.epias.com.tr)  ·  "
    "Pipeline: Bruin  ·  Database: BigQuery  ·  Visualization: Streamlit + Altair"
)
