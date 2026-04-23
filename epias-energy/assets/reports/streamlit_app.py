from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
from google.cloud import bigquery

st.set_page_config(
    page_title="Turkey Energy Market Dashboard",
    layout="wide",
)

PROJECT_ID = "bruin-playground-arsalan"
base_path = Path(__file__).parent

# ---------------------------------------------------------------------------
# IBM Carbon Design System — Data Visualization Categorical Palette
# Source: github.com/carbon-design-system/carbon  packages/colors/src/colors.ts
# Colors are sequenced for maximum perceptual distance under deuteranopia,
# protanopia, and tritanopia.  Use in order: cat1 first, then cat2, etc.
# ---------------------------------------------------------------------------
IBM_CAT = [
    "#6929c4",  # 1  Purple 70
    "#1192e8",  # 2  Cyan 50
    "#005d5d",  # 3  Teal 70
    "#9f1853",  # 4  Magenta 70
    "#fa4d56",  # 5  Red 50
    "#520408",  # 6  Red 90
    "#198038",  # 7  Green 60
    "#002d9c",  # 8  Blue 80
    "#ee5396",  # 9  Magenta 50
    "#b28600",  # 10 Yellow 50
    "#8a3800",  # 11 Orange 70
    "#a56eff",  # 12 Purple 50
    "#009d9a",  # 13 Teal 50
    "#0f62fe",  # 14 Blue 60
]

# Semantic tokens (IBM Carbon)
IBM_POSITIVE = "#198038"   # Green 60
IBM_NEGATIVE = "#fa4d56"   # Red 50
IBM_NEUTRAL = "#878d96"    # Gray 50

# Source-level colors: brighter IBM Carbon variants for maximum visual separation
SOURCE_COLORS = {
    "natural_gas":   "#6929c4",  # Purple 70
    "dammed_hydro":  "#1192e8",  # Cyan 50
    "lignite":       "#009d9a",  # Teal 50
    "wind":          "#ee5396",  # Magenta 50
    "hard_coal":     "#fa4d56",  # Red 50
    "solar":         "#f1c21b",  # Yellow 30
    "river":         "#0f62fe",  # Blue 60
    "geothermal":    "#198038",  # Green 60
    "biomass":       "#8a3800",  # Orange 70
    "fuel_oil":      "#a56eff",  # Purple 50
    "naphta":        "#b28600",  # Yellow 50
    "import_export": IBM_NEUTRAL,
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

# Category-level colors
CATEGORY_MAP = {
    "solar": "Solar",
    "dammed_hydro": "Hydro",
    "river": "Hydro",
    "wind": "Wind",
    "geothermal": "Geothermal",
    "biomass": "Biomass",
    "natural_gas": "Non-Renewables",
    "lignite": "Non-Renewables",
    "hard_coal": "Non-Renewables",
    "fuel_oil": "Non-Renewables",
    "naphta": "Non-Renewables",
}
RENEWABLE_CATEGORIES = {"Solar", "Wind", "Hydro", "Geothermal", "Biomass"}
CATEGORY_ORDER = ["Hydro", "Wind", "Solar", "Geothermal", "Biomass", "Non-Renewables"]
CATEGORY_COLORS = [
    "#1192e8",    # Hydro       — Cyan 50
    "#ee5396",    # Wind        — Magenta 50
    "#f1c21b",    # Solar       — Yellow 30
    "#fa4d56",    # Geothermal  — Red 50
    "#198038",    # Biomass     — Green 60
    IBM_NEUTRAL,  # Non-Renew   — Gray 50
]

# Stroke dash patterns for line charts (IBM: use shape + color for CVD safety)
CATEGORY_DASHES = {
    "Solar":       [1, 0],
    "Wind":        [6, 4],
    "Hydro":       [2, 2],
    "Geothermal":  [6, 2, 2, 2],
    "Biomass":     [8, 4, 2, 4],
}


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

@st.cache_resource
def get_client():
    return bigquery.Client(project=PROJECT_ID)


def run_raw(sql: str) -> pd.DataFrame:
    return get_client().query(sql).to_dataframe()


@st.cache_data(ttl=3600)
def load_generation():
    return run_raw(
        "SELECT * FROM `bruin-playground-arsalan.epias_staging.epias_generation_daily` ORDER BY date"
    )


@st.cache_data(ttl=3600)
def load_forecast():
    return run_raw(
        "SELECT * FROM `bruin-playground-arsalan.epias_staging.epias_forecast_vs_actual` ORDER BY date"
    )


@st.cache_data(ttl=3600)
def load_prices():
    return run_raw(
        "SELECT * FROM `bruin-playground-arsalan.epias_staging.epias_market_prices_daily` ORDER BY date"
    )


@st.cache_data(ttl=3600)
def load_h1():
    return run_raw(
        "SELECT * FROM `bruin-playground-arsalan.epias_staging.h1_lira_coal_shift` ORDER BY year, month"
    )


@st.cache_data(ttl=3600)
def load_h2():
    return run_raw(
        "SELECT * FROM `bruin-playground-arsalan.epias_staging.h2_drought_hydro_mcp` ORDER BY year, month"
    )


@st.cache_data(ttl=3600)
def load_h3():
    return run_raw(
        "SELECT * FROM `bruin-playground-arsalan.epias_staging.h3_hormuz_risk_mcp` ORDER BY date"
    )


@st.cache_data(ttl=3600)
def load_h4():
    return run_raw(
        "SELECT * FROM `bruin-playground-arsalan.epias_staging.h4_renewable_demand_driven` ORDER BY year"
    )


# ---------------------------------------------------------------------------
# Helper: structured footnote (Sources, Tools, Methodology, Limitations)
# ---------------------------------------------------------------------------

def chart_footnote(*, sources: str, tools: str, methodology: str, limitations: str):
    text = (
        f"<b>Sources:</b> {sources}<br>"
        f"<b>Tools:</b> {tools}<br>"
        f"<b>Methodology:</b> {methodology}<br>"
        f"<b>Limitations:</b> {limitations}"
    )
    st.markdown(
        f"<div style='font-size:0.75rem; color:#878d96; margin-top:0.25rem; line-height:1.5;'>{text}</div>",
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Page header
# ---------------------------------------------------------------------------

st.title("Turkey Energy Market Dashboard")
st.caption(
    "EPIAS Transparency Platform data  |  "
    "Built with Bruin + BigQuery + Streamlit"
)

tab_gen, tab_forecast, tab_prices, tab_hypo = st.tabs([
    "Generation by Source",
    "Forecast vs Actual",
    "Market Prices",
    "Socioeconomic Analysis",
])


# ======================================================================
# Tab 1: Generation by Source
# ======================================================================

with tab_gen:
    gen = load_generation()
    gen["date"] = pd.to_datetime(gen["date"])
    gen_sources = gen[gen["source_name"] != "import_export"].copy()
    gen_sources["source_label"] = gen_sources["source_name"].map(SOURCE_LABELS)

    min_year = gen_sources["date"].dt.year.min()
    max_year = gen_sources["date"].dt.year.max()

    st.subheader("Turkey Electricity Generation by Source")

    total_by_source = (
        gen_sources.groupby("source_name")["generation_mwh"]
        .sum()
        .sort_values(ascending=False)
    )

    col1, col2, col3 = st.columns(3)
    total_gen = total_by_source.sum()
    with col1:
        st.metric("Total Generation", f"{total_gen / 1e6:,.1f} TWh")
    with col2:
        top_source = total_by_source.index[0]
        top_pct = total_by_source.iloc[0] / total_gen * 100
        st.metric("Largest Source", SOURCE_LABELS.get(top_source, top_source), f"{top_pct:.1f}%")
    with col3:
        renewables = total_by_source[
            total_by_source.index.isin(["wind", "solar", "geothermal", "biomass", "river", "dammed_hydro"])
        ].sum()
        ren_pct = renewables / total_gen * 100
        st.metric("Renewable Share", f"{ren_pct:.1f}%")

    st.markdown("---")

    # --- Prepare category data (used by multiple charts) ---
    cat_data = gen_sources.copy()
    cat_data["category"] = cat_data["source_name"].map(CATEGORY_MAP)
    cat_data = cat_data.dropna(subset=["category"])
    cat_yearly = (
        cat_data
        .assign(year=cat_data["date"].dt.year)
        .groupby(["year", "category"])["generation_mwh"]
        .sum()
        .reset_index()
    )
    year_totals = cat_yearly.groupby("year")["generation_mwh"].sum().rename("total_mwh")
    cat_yearly = cat_yearly.merge(year_totals, on="year")
    cat_yearly["share_pct"] = (cat_yearly["generation_mwh"] / cat_yearly["total_mwh"] * 100).round(2)

    # --- Chart 1: Annual fuel mix line chart (moved to top) ---
    st.markdown("**Annual Share of Electricity Generation by Fuel Category (%)**")
    st.caption(
        "Gas share declined from 33% (2016) to 15-25% (2024-2025) while renewables "
        "(excl. hydro) rose from 8% to 21%. Coal has remained in a 31-38% band. "
        "Hydro varies year-to-year with rainfall. These trends reflect the structural "
        "shift in Turkey's electricity supply from imported gas toward domestic coal "
        "and expanding wind/solar capacity."
    )

    gen_fuel = gen_sources.copy()
    gen_fuel["year"] = gen_fuel["date"].dt.year
    gen_fuel = gen_fuel.query("year > @min_year and year < @max_year")
    gen_fuel["fuel_group"] = gen_fuel["source_name"].map({
        "natural_gas": "Gas",
        "lignite": "Coal",
        "hard_coal": "Coal",
        "dammed_hydro": "Hydro",
        "river": "Hydro",
        "wind": "Renewables (excl. Hydro)",
        "solar": "Renewables (excl. Hydro)",
        "geothermal": "Renewables (excl. Hydro)",
        "biomass": "Renewables (excl. Hydro)",
        "fuel_oil": "Other Fossil",
        "naphta": "Other Fossil",
    })
    gen_fuel = gen_fuel.dropna(subset=["fuel_group"])
    fuel_yearly = gen_fuel.groupby(["year", "fuel_group"])["generation_mwh"].sum().reset_index()
    fuel_totals = fuel_yearly.groupby("year")["generation_mwh"].sum().rename("total_mwh")
    fuel_yearly = fuel_yearly.merge(fuel_totals, on="year")
    fuel_yearly["share_pct"] = (fuel_yearly["generation_mwh"] / fuel_yearly["total_mwh"] * 100).round(1)

    fuel_mix_domain = ["Gas", "Coal", "Hydro", "Renewables (excl. Hydro)", "Other Fossil"]
    fuel_mix_colors = ["#6929c4", "#009d9a", "#1192e8", "#198038", IBM_NEUTRAL]
    fuel_mix_dashes = [[1, 0], [6, 4], [2, 2], [6, 2, 2, 2], [8, 4, 2, 4]]

    fuel_mix_lines = (
        alt.Chart(fuel_yearly)
        .mark_line(strokeWidth=2.5, point=alt.OverlayMarkDef(size=50))
        .encode(
            x=alt.X("year:O", title="Year"),
            y=alt.Y("share_pct:Q", title="Share of Total Generation (%)"),
            color=alt.Color(
                "fuel_group:N",
                title="Fuel Category",
                scale=alt.Scale(domain=fuel_mix_domain, range=fuel_mix_colors),
                sort=fuel_mix_domain,
                legend=alt.Legend(
                    orient="right",
                    symbolType="stroke",
                    symbolSize=120,
                    symbolStrokeWidth=2.5,
                    labelFontSize=11,
                    titleFontSize=12,
                ),
            ),
            strokeDash=alt.StrokeDash(
                "fuel_group:N",
                scale=alt.Scale(domain=fuel_mix_domain, range=fuel_mix_dashes),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("year:O", title="Year"),
                alt.Tooltip("fuel_group:N", title="Fuel Category"),
                alt.Tooltip("share_pct:Q", title="Share of Total (%)", format=".1f"),
                alt.Tooltip("generation_mwh:Q", title="Generation (MWh)", format=",.0f"),
            ],
        )
        .properties(height=400)
    )
    st.altair_chart(fuel_mix_lines, use_container_width=True)
    chart_footnote(
        sources="EPIAS Transparency Platform (seffaflik.epias.com.tr), real-time generation endpoint.",
        tools="Bruin pipeline, BigQuery, Altair. IBM Carbon categorical palette for CVD accessibility.",
        methodology=(
            "Daily source-level MWh summed to annual totals per fuel group, then divided by total annual MWh. "
            "Coal = lignite + hard coal. Renewables excl. Hydro = wind + solar + geothermal + biomass. "
            "Other Fossil = fuel oil + naphta. Import/export excluded."
        ),
        limitations=f"Incomplete boundary years ({min_year}, {max_year}) excluded. Line dash patterns supplement color for accessibility.",
    )

    st.markdown("---")

    # --- Chart 2: Stacked bar by source ---
    st.markdown("**Annual Total Electricity Generation by Energy Source (MWh)**")
    st.caption(
        "Stacked bars show each source's absolute contribution to Turkey's annual "
        "electricity output. Natural gas is the largest single source, followed by "
        "lignite and dammed hydro. Total generation has grown from ~264 TWh (2016) "
        "to ~317 TWh (2025)."
    )

    yearly_source = (
        gen_sources.assign(year=gen_sources["date"].dt.year)
        .query("year > @min_year and year < @max_year")
        .groupby(["year", "source_name", "source_label"])["generation_mwh"]
        .sum()
        .reset_index()
    )

    source_order = total_by_source.index.tolist()
    source_labels_ordered = [SOURCE_LABELS.get(s, s) for s in source_order]
    source_color_values = [SOURCE_COLORS.get(s, IBM_NEUTRAL) for s in source_order]

    stacked_bar = (
        alt.Chart(yearly_source)
        .mark_bar()
        .encode(
            x=alt.X("year:O", title="Year"),
            y=alt.Y("generation_mwh:Q", title="Generation (MWh)", stack="zero"),
            color=alt.Color(
                "source_label:N",
                title="Energy Source",
                scale=alt.Scale(domain=source_labels_ordered, range=source_color_values),
                sort=source_labels_ordered,
                legend=alt.Legend(
                    orient="right",
                    columns=1,
                    symbolType="square",
                    symbolSize=120,
                    labelFontSize=11,
                    titleFontSize=12,
                ),
            ),
            tooltip=[
                alt.Tooltip("year:O", title="Year"),
                alt.Tooltip("source_label:N", title="Energy Source"),
                alt.Tooltip("generation_mwh:Q", title="Generation (MWh)", format=",.0f"),
            ],
        )
        .properties(height=420)
    )
    st.altair_chart(stacked_bar, use_container_width=True)
    chart_footnote(
        sources="EPIAS Transparency Platform, real-time generation endpoint.",
        tools="Bruin pipeline, BigQuery, Altair.",
        methodology=f"Daily source-level MWh aggregated to annual totals. Import/export excluded. Incomplete boundary years ({min_year}, {max_year}) excluded.",
        limitations="Does not account for capacity factor, installed capacity, or curtailment. MWh reflects dispatched generation only.",
    )

    st.markdown("---")

    # --- Chart 3: Category stacked % ---
    st.markdown("**Annual Generation Mix by Source Category (% of Total)**")
    st.caption(
        "Percentage breakdown shows the relative contribution of each source "
        "category. Hydro is the largest renewable but varies 18-31% with rainfall. "
        "Wind has grown from 6% to 12%. Non-renewables still account for ~58-67% "
        "of annual generation."
    )

    stacked_pct = (
        alt.Chart(cat_yearly)
        .mark_bar()
        .encode(
            x=alt.X("year:O", title="Year"),
            y=alt.Y("share_pct:Q", title="Share of Generation (%)", stack="zero"),
            color=alt.Color(
                "category:N",
                title="Source Category",
                scale=alt.Scale(domain=CATEGORY_ORDER, range=CATEGORY_COLORS),
                sort=CATEGORY_ORDER,
                legend=alt.Legend(
                    orient="right",
                    symbolType="square",
                    symbolSize=120,
                    labelFontSize=11,
                    titleFontSize=12,
                ),
            ),
            tooltip=[
                alt.Tooltip("year:O", title="Year"),
                alt.Tooltip("category:N", title="Source Category"),
                alt.Tooltip("share_pct:Q", title="Share of Total (%)", format=".1f"),
                alt.Tooltip("generation_mwh:Q", title="Generation (MWh)", format=",.0f"),
            ],
        )
        .properties(height=420)
    )
    st.altair_chart(stacked_pct, use_container_width=True)
    chart_footnote(
        sources="EPIAS Transparency Platform.",
        tools="Bruin pipeline, BigQuery, Altair.",
        methodology="Hydro = dammed + run-of-river. Non-Renewables = natural gas + lignite + hard coal + fuel oil + naphta. Shares = category MWh / total annual MWh.",
        limitations="All years shown including partial boundary years, which may understate totals. Import/export excluded.",
    )

    st.markdown("---")

    # --- Chart 4: Renewable vs Non-Renewable ---
    st.markdown("**Combined Renewable vs Non-Renewable Share of Annual Generation (%)**")
    st.caption(
        "Renewables (solar, wind, hydro, geothermal, biomass) combined have "
        "fluctuated between 29% and 43% of total generation. The year-to-year "
        "variation is primarily driven by hydro output, which depends on annual "
        "rainfall patterns."
    )

    binary_yearly = cat_yearly.query("year > @min_year").copy()
    binary_yearly["group"] = binary_yearly["category"].apply(
        lambda c: "Renewables" if c in RENEWABLE_CATEGORIES else "Non-Renewables"
    )
    binary_yearly = (
        binary_yearly.groupby(["year", "group"])
        .agg(generation_mwh=("generation_mwh", "sum"), total_mwh=("total_mwh", "first"))
        .reset_index()
    )
    binary_yearly["share_pct"] = (binary_yearly["generation_mwh"] / binary_yearly["total_mwh"] * 100).round(2)

    binary_chart = (
        alt.Chart(binary_yearly)
        .mark_bar()
        .encode(
            x=alt.X("year:O", title="Year"),
            y=alt.Y("share_pct:Q", title="Share of Generation (%)", stack="zero"),
            color=alt.Color(
                "group:N",
                title="Category",
                scale=alt.Scale(
                    domain=["Renewables", "Non-Renewables"],
                    range=[IBM_CAT[1], IBM_NEUTRAL],
                ),
                sort=["Renewables", "Non-Renewables"],
                legend=alt.Legend(
                    orient="right",
                    symbolType="square",
                    symbolSize=120,
                    labelFontSize=11,
                    titleFontSize=12,
                ),
            ),
            tooltip=[
                alt.Tooltip("year:O", title="Year"),
                alt.Tooltip("group:N", title="Category"),
                alt.Tooltip("share_pct:Q", title="Share of Total (%)", format=".1f"),
                alt.Tooltip("generation_mwh:Q", title="Generation (MWh)", format=",.0f"),
            ],
        )
        .properties(height=400)
    )
    st.altair_chart(binary_chart, use_container_width=True)
    chart_footnote(
        sources="EPIAS Transparency Platform.",
        tools="Bruin pipeline, BigQuery, Altair.",
        methodology="Renewables = solar + wind + hydro (dammed + river) + geothermal + biomass. Non-Renewables = natural gas + lignite + hard coal + fuel oil + naphta. Shares sum to 100% each year.",
        limitations="Import/export excluded from both categories. Does not distinguish between large hydro and small hydro.",
    )

    st.markdown("---")

    # --- Chart 5: Renewable generation TWh by source ---
    renew_order = [c for c in CATEGORY_ORDER if c in RENEWABLE_CATEGORIES]
    renew_colors = [CATEGORY_COLORS[CATEGORY_ORDER.index(c)] for c in renew_order]

    st.markdown("**Annual Renewable Electricity Generation by Source (TWh)**")
    st.caption(
        "Hydro output ranges from 55 to 89 TWh per year depending on rainfall. "
        "Wind generation has grown from 12 TWh (2016) to 39 TWh (2025). Solar has "
        "risen from near-zero to 8 TWh. Geothermal and biomass each contribute "
        "under 3 TWh annually."
    )

    renew_yearly = cat_yearly[cat_yearly["category"].isin(RENEWABLE_CATEGORIES)].copy()
    renew_yearly = renew_yearly.query("year > @min_year and year < @max_year")
    renew_yearly["generation_twh"] = (renew_yearly["generation_mwh"] / 1e6).round(3)

    renew_lines = (
        alt.Chart(renew_yearly)
        .mark_line(strokeWidth=2.5, point=alt.OverlayMarkDef(size=50))
        .encode(
            x=alt.X("year:O", title="Year"),
            y=alt.Y("generation_twh:Q", title="Generation (TWh)"),
            color=alt.Color(
                "category:N",
                title="Renewable Source",
                scale=alt.Scale(domain=renew_order, range=renew_colors),
                sort=renew_order,
                legend=alt.Legend(
                    orient="right",
                    symbolType="stroke",
                    symbolSize=120,
                    symbolStrokeWidth=2.5,
                    labelFontSize=11,
                    titleFontSize=12,
                ),
            ),
            strokeDash=alt.StrokeDash(
                "category:N",
                scale=alt.Scale(domain=renew_order, range=[CATEGORY_DASHES[c] for c in renew_order]),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("year:O", title="Year"),
                alt.Tooltip("category:N", title="Source"),
                alt.Tooltip("generation_twh:Q", title="Generation (TWh)", format=".2f"),
                alt.Tooltip("generation_mwh:Q", title="Generation (MWh)", format=",.0f"),
            ],
        )
        .properties(height=400)
    )
    st.altair_chart(renew_lines, use_container_width=True)
    chart_footnote(
        sources="EPIAS Transparency Platform, real-time generation endpoint.",
        tools="Bruin pipeline, BigQuery, Altair. Line dash patterns per IBM CVD accessibility guidelines.",
        methodology=f"Daily MWh summed to annual totals, converted to TWh (/ 1,000,000). Hydro = dammed + run-of-river combined. Incomplete boundary years ({min_year}, {max_year}) excluded.",
        limitations="Reflects dispatched generation, not installed capacity. Hydro variability is weather-driven, not a capacity trend.",
    )

    st.markdown("---")

    # --- Chart 6: Renewable share % by source ---
    st.markdown("**Each Renewable Source's Share of Total Annual Generation (%)**")
    st.caption(
        "Hydro's share fluctuates between 18% and 31% depending on annual rainfall. "
        "Wind has grown from 6% to 12%. Solar remains below 3% but has the fastest "
        "growth rate among renewables. Geothermal and biomass each contribute under "
        "2% of total generation."
    )

    renew_share_by_source = (
        cat_yearly[cat_yearly["category"].isin(RENEWABLE_CATEGORIES)]
        .query("year > @min_year and year < @max_year")
        .copy()
    )

    renew_pct_lines = (
        alt.Chart(renew_share_by_source)
        .mark_line(strokeWidth=2.5, point=alt.OverlayMarkDef(size=50))
        .encode(
            x=alt.X("year:O", title="Year"),
            y=alt.Y("share_pct:Q", title="Share of Total Generation (%)"),
            color=alt.Color(
                "category:N",
                title="Renewable Source",
                scale=alt.Scale(domain=renew_order, range=renew_colors),
                sort=renew_order,
                legend=alt.Legend(
                    orient="right",
                    symbolType="stroke",
                    symbolSize=120,
                    symbolStrokeWidth=2.5,
                    labelFontSize=11,
                    titleFontSize=12,
                ),
            ),
            strokeDash=alt.StrokeDash(
                "category:N",
                scale=alt.Scale(domain=renew_order, range=[CATEGORY_DASHES[c] for c in renew_order]),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("year:O", title="Year"),
                alt.Tooltip("category:N", title="Source"),
                alt.Tooltip("share_pct:Q", title="Share of Total (%)", format=".1f"),
                alt.Tooltip("generation_mwh:Q", title="Generation (MWh)", format=",.0f"),
            ],
        )
        .properties(height=400)
    )
    st.altair_chart(renew_pct_lines, use_container_width=True)
    chart_footnote(
        sources="EPIAS Transparency Platform.",
        tools="Bruin pipeline, BigQuery, Altair.",
        methodology=f"Share = source category MWh / total annual generation MWh (all sources excl. import/export). Incomplete boundary years ({min_year}, {max_year}) excluded.",
        limitations="Hydro variability is weather-driven and should not be interpreted as a policy or investment trend. Wind and solar growth reflects new installed capacity.",
    )


# ======================================================================
# Tab 2: Forecast vs Actual
# ======================================================================

with tab_forecast:
    fva = load_forecast()
    fva["date"] = pd.to_datetime(fva["date"])

    st.subheader("Day-Ahead Forecast vs Actual Generation")

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

        # --- Chart: Monthly forecast vs actual ---
        st.markdown("**Monthly Total Generation: Day-Ahead Plan vs Actual Output (MWh)**")
        st.caption(
            "The DPP (Day-ahead Production Plan, first version) tracks actual generation "
            "closely at the monthly level, with divergences typically under 5%. The "
            "two lines overlap for most months, indicating consistent forecasting accuracy."
        )

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
            "forecast_mwh": "Forecast (DPP First Version)",
            "actual_mwh": "Actual Generation",
        })

        fva_lines = (
            alt.Chart(monthly_fva_long)
            .mark_line(strokeWidth=2.5)
            .encode(
                x=alt.X("month:T", title="Month"),
                y=alt.Y("mwh:Q", title="Total Generation (MWh)"),
                color=alt.Color(
                    "type:N",
                    title="Series",
                    scale=alt.Scale(
                        domain=["Forecast (DPP First Version)", "Actual Generation"],
                        range=[IBM_CAT[0], IBM_CAT[1]],
                    ),
                    legend=alt.Legend(
                        orient="top",
                        symbolType="stroke",
                        symbolSize=120,
                        symbolStrokeWidth=2.5,
                        labelFontSize=11,
                        titleFontSize=12,
                    ),
                ),
                strokeDash=alt.StrokeDash(
                    "type:N",
                    scale=alt.Scale(
                        domain=["Forecast (DPP First Version)", "Actual Generation"],
                        range=[[6, 4], [1, 0]],
                    ),
                    legend=None,
                ),
                tooltip=[
                    alt.Tooltip("month:T", title="Month", format="%b %Y"),
                    alt.Tooltip("type:N", title="Series"),
                    alt.Tooltip("mwh:Q", title="Generation (MWh)", format=",.0f"),
                ],
            )
            .properties(height=380)
        )
        st.altair_chart(fva_lines, use_container_width=True)
        chart_footnote(
            sources="EPIAS Transparency Platform, DPP (first version) and real-time generation endpoints.",
            tools="Bruin pipeline, BigQuery, Altair.",
            methodology="Daily forecasts and actuals summed to monthly totals. Forecast = first published day-ahead plan, not the final revised version. Incomplete current month excluded.",
            limitations="Only first-version DPP is compared; later revisions may improve accuracy. Aggregation to monthly level masks daily forecast errors.",
        )

        st.markdown("---")

        # --- Chart: Error by source ---
        st.markdown("**Mean Daily Forecast Error by Energy Source (%)**")
        st.caption(
            "Positive values indicate over-forecasting (planned more than produced); "
            "negative values indicate under-forecasting. Variable sources (wind, solar, "
            "hydro) tend to show larger deviations than dispatchable sources (gas, coal)."
        )

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
                x=alt.X("error_pct:Q", title="Mean Daily Forecast Error (%)"),
                y=alt.Y(
                    "source_label:N",
                    title=None,
                    sort=alt.EncodingSortField(field="error_pct", order="ascending"),
                ),
                color=alt.condition(
                    alt.datum.error_pct > 0,
                    alt.value(IBM_NEGATIVE),
                    alt.value(IBM_CAT[1]),
                ),
                tooltip=[
                    alt.Tooltip("source_label:N", title="Energy Source"),
                    alt.Tooltip("error_pct:Q", title="Mean Forecast Error (%)", format="+.2f"),
                ],
            )
            .properties(height=340)
        )

        zero_line = (
            alt.Chart(pd.DataFrame({"x": [0]}))
            .mark_rule(color="#393939", strokeWidth=1)
            .encode(x="x:Q")
        )
        st.altair_chart(error_bars + zero_line, use_container_width=True)
        chart_footnote(
            sources="EPIAS Transparency Platform, DPP and real-time generation endpoints.",
            tools="Bruin pipeline, BigQuery, Altair.",
            methodology="Error = (forecast - actual) / actual * 100, averaged across all days. Red = over-forecast; blue = under-forecast.",
            limitations="Based on DPP first version only. Mean error can mask variability; sources with high daily variance may have low mean error but high absolute error.",
        )
    else:
        st.warning("No total forecast data available.")


# ======================================================================
# Tab 3: Market Prices
# ======================================================================

with tab_prices:
    prices = load_prices()
    prices["date"] = pd.to_datetime(prices["date"])

    st.subheader("Day-Ahead and Balancing Market Electricity Prices")

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

        # --- Chart: Daily MCP vs SMP ---
        st.markdown("**Daily Average MCP and SMP Electricity Prices (TRY/MWh)**")
        st.caption(
            "MCP (Market Clearing Price) is the day-ahead market price; SMP (System "
            "Marginal Price) is the real-time balancing price. Both have risen sharply "
            "in nominal TRY terms since 2020, reflecting Lira depreciation and commodity "
            "price increases. The two series generally track each other."
        )

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
                        range=[IBM_CAT[1], IBM_CAT[4]],
                    ),
                    legend=alt.Legend(
                        orient="top",
                        symbolType="stroke",
                        symbolSize=120,
                        symbolStrokeWidth=2.5,
                        labelFontSize=11,
                        titleFontSize=12,
                    ),
                ),
                tooltip=[
                    alt.Tooltip("date:T", title="Date", format="%b %d, %Y"),
                    alt.Tooltip("price_type:N", title="Price Type"),
                    alt.Tooltip("try_mwh:Q", title="Price (TRY/MWh)", format=",.1f"),
                ],
            )
            .properties(height=380)
        )
        st.altair_chart(price_lines, use_container_width=True)
        chart_footnote(
            sources="EPIAS Transparency Platform, day-ahead market (MCP) and balancing market (SMP) endpoints.",
            tools="Bruin pipeline, BigQuery, Altair.",
            methodology="Daily average of hourly prices. TRY = Turkish Lira. Incomplete current month excluded.",
            limitations="Prices are in nominal TRY, not inflation-adjusted. The Lira depreciated ~15x against USD (2015-2025), so nominal increases partly reflect currency depreciation. SMP data may be unavailable for the most recent 2-3 weeks.",
        )

        st.markdown("---")

        # --- Chart: Daily spread ---
        st.markdown("**Daily Balancing Market Premium: SMP minus MCP (TRY/MWh)**")
        st.caption(
            "Positive spread means real-time balancing electricity cost more than "
            "the day-ahead market price. Large negative values indicate oversupply "
            "pushing real-time prices below day-ahead levels."
        )

        spread_bars = (
            alt.Chart(prices)
            .mark_bar()
            .encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("spread_avg:Q", title="Spread: SMP - MCP (TRY/MWh)"),
                color=alt.condition(
                    alt.datum.spread_avg > 0,
                    alt.value(IBM_NEGATIVE),
                    alt.value(IBM_CAT[1]),
                ),
                tooltip=[
                    alt.Tooltip("date:T", title="Date", format="%b %d, %Y"),
                    alt.Tooltip("spread_avg:Q", title="Spread (TRY/MWh)", format="+,.1f"),
                    alt.Tooltip("mcp_avg:Q", title="MCP (TRY/MWh)", format=",.1f"),
                    alt.Tooltip("smp_avg:Q", title="SMP (TRY/MWh)", format=",.1f"),
                ],
            )
            .properties(height=340)
        )

        zero_line = (
            alt.Chart(pd.DataFrame({"y": [0]}))
            .mark_rule(color="#393939", strokeWidth=1)
            .encode(y="y:Q")
        )
        st.altair_chart(spread_bars + zero_line, use_container_width=True)
        chart_footnote(
            sources="EPIAS Transparency Platform.",
            tools="Bruin pipeline, BigQuery, Altair.",
            methodology="Spread = daily average SMP - daily average MCP. Red = balancing premium (SMP > MCP); blue = discount (SMP < MCP). Incomplete current month excluded.",
            limitations="Daily averaging masks intra-day spread volatility. Spread can be driven by forecast errors, unexpected outages, or demand shifts not visible in daily data.",
        )

        st.markdown("---")

        # --- Chart: Monthly avg MCP vs SMP ---
        st.markdown("**Monthly Average MCP and SMP Electricity Prices (TRY/MWh)**")
        st.caption(
            "Monthly aggregation smooths daily volatility. The 2022 peak reflects "
            "the Lira crisis and global energy price increases. Prices have moderated "
            "since but remain above pre-2020 levels in nominal TRY."
        )

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
            "mcp_avg": "MCP (Day-Ahead)",
            "smp_avg": "SMP (Balancing)",
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
                        domain=["MCP (Day-Ahead)", "SMP (Balancing)"],
                        range=[IBM_CAT[1], IBM_CAT[4]],
                    ),
                    legend=alt.Legend(
                        orient="top",
                        symbolType="square",
                        symbolSize=120,
                        labelFontSize=11,
                        titleFontSize=12,
                    ),
                ),
                xOffset="type:N",
                tooltip=[
                    alt.Tooltip("month:T", title="Month", format="%b %Y"),
                    alt.Tooltip("type:N", title="Price Type"),
                    alt.Tooltip("avg_price:Q", title="Avg Price (TRY/MWh)", format=",.1f"),
                ],
            )
            .properties(height=340)
        )
        st.altair_chart(monthly_bar, use_container_width=True)
        chart_footnote(
            sources="EPIAS Transparency Platform.",
            tools="Bruin pipeline, BigQuery, Altair.",
            methodology="Monthly average of daily average prices. Grouped bars show MCP and SMP side by side for each month.",
            limitations="Nominal TRY prices, not inflation- or FX-adjusted. Monthly averaging obscures daily and hourly price spikes.",
        )
    else:
        st.warning("No price data available.")


# ======================================================================
# Tab 4: Socioeconomic Hypotheses
# ======================================================================

with tab_hypo:
    st.subheader("Socioeconomic Cross-Reference Analysis")
    st.caption(
        "Four hypotheses tested by joining EPIAS electricity data with external "
        "datasets: FRED exchange rates, Open-Meteo weather, Polymarket prediction "
        "markets, World Bank development indicators, and commodity prices."
    )

    # ------------------------------------------------------------------
    # H1: Lira depreciation and fuel mix
    # ------------------------------------------------------------------
    st.markdown("---")
    st.markdown("**Monthly Gas, Coal, and Renewable Shares of Generation vs TRY/USD Exchange Rate (2016-2025)**")
    st.caption(
        "Gas share declined from ~39% to ~15-25% over this period while the Lira "
        "depreciated from 3 to 44 TRY/USD. Since Turkey imports most natural gas "
        "(priced in USD), currency depreciation raises gas generation costs. Coal "
        "(domestically sourced) remained stable at 31-38%. Renewables (excl. hydro) "
        "rose from ~8% to ~23%."
    )

    h1 = load_h1()
    h1["date"] = pd.to_datetime(h1[["year", "month"]].assign(day=1))
    h1_plot = h1[(h1["year"] >= 2016) & (h1["year"] <= 2025)].copy()

    h1_fuel = h1_plot.melt(
        id_vars=["date", "tryusd"],
        value_vars=["gas_share_pct", "coal_share_pct", "renewable_share_pct"],
        var_name="fuel",
        value_name="share_pct",
    )
    h1_fuel["fuel"] = h1_fuel["fuel"].map({
        "gas_share_pct": "Natural Gas",
        "coal_share_pct": "Coal (Lignite + Hard Coal)",
        "renewable_share_pct": "Renewables (excl. Hydro)",
    })

    h1_fuel_domain = ["Natural Gas", "Coal (Lignite + Hard Coal)", "Renewables (excl. Hydro)"]
    h1_fuel_colors = ["#6929c4", "#009d9a", "#198038"]
    h1_fuel_dashes = [[1, 0], [6, 4], [2, 2]]

    fuel_lines = (
        alt.Chart(h1_fuel)
        .mark_line(strokeWidth=2)
        .encode(
            x=alt.X("date:T", title="Month"),
            y=alt.Y("share_pct:Q", title="Share of Generation (%)"),
            color=alt.Color(
                "fuel:N",
                title="Fuel Type",
                scale=alt.Scale(domain=h1_fuel_domain, range=h1_fuel_colors),
                legend=alt.Legend(
                    orient="top",
                    symbolType="stroke",
                    symbolSize=120,
                    symbolStrokeWidth=2.5,
                    labelFontSize=11,
                    titleFontSize=12,
                    direction="horizontal",
                ),
            ),
            strokeDash=alt.StrokeDash(
                "fuel:N",
                scale=alt.Scale(domain=h1_fuel_domain, range=h1_fuel_dashes),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("date:T", title="Month", format="%b %Y"),
                alt.Tooltip("fuel:N", title="Fuel Type"),
                alt.Tooltip("share_pct:Q", title="Share of Generation (%)", format=".1f"),
                alt.Tooltip("tryusd:Q", title="TRY/USD Rate", format=".2f"),
            ],
        )
        .properties(height=380)
    )

    fx_area = (
        alt.Chart(h1_plot)
        .mark_area(opacity=0.15, color=IBM_CAT[4])
        .encode(
            x=alt.X("date:T", title="Month"),
            y=alt.Y("tryusd:Q", title="TRY per USD (monthly avg)"),
            tooltip=[
                alt.Tooltip("date:T", title="Month", format="%b %Y"),
                alt.Tooltip("tryusd:Q", title="TRY/USD", format=".2f"),
            ],
        )
        .properties(height=380)
    )

    h1_col1, h1_col2 = st.columns([3, 2])
    with h1_col1:
        st.altair_chart(fuel_lines, use_container_width=True)
    with h1_col2:
        st.altair_chart(fx_area, use_container_width=True)

    chart_footnote(
        sources="EPIAS Transparency Platform (generation). FRED series CCUSMA02TRM618N (monthly avg TRY/USD, OECD via FRED).",
        tools="Bruin pipeline, BigQuery, Altair. FRED API for exchange rate extraction.",
        methodology="Coal = lignite + hard coal. Renewables excl. hydro = wind + solar + geothermal + biomass. Hydro excluded because its variability is weather-driven, not FX-driven. Exchange rate is monthly average, not end-of-month spot.",
        limitations="Correlation does not imply causation. Other factors (policy changes, global gas prices, weather, new capacity additions) also affect fuel mix. Only 120 monthly data points.",
    )

    h1_annual = (
        h1_plot.groupby("year")
        .agg(
            tryusd=("tryusd", "mean"),
            coal_share_pct=("coal_share_pct", "mean"),
            gas_share_pct=("gas_share_pct", "mean"),
            renewable_share_pct=("renewable_share_pct", "mean"),
            mcp_avg_eur=("mcp_avg_eur", "mean"),
        )
        .round(2)
        .reset_index()
    )
    h1_annual.columns = ["Year", "Avg TRY/USD", "Avg Coal Share (%)", "Avg Gas Share (%)",
                          "Avg Renewable Share (%)", "Avg MCP (EUR/MWh)"]
    st.dataframe(h1_annual, hide_index=True, use_container_width=True)

    # ------------------------------------------------------------------
    # H2: Drought, hydro, and electricity prices
    # ------------------------------------------------------------------
    st.markdown("---")
    st.markdown("**Monthly Precipitation Anomaly Relative to Long-Term Average for Turkey (2016-2025)**")
    st.caption(
        "Turkey experienced persistent below-average rainfall from 2017 onward. "
        "Months with severe drought (>30% below normal) had an average hydro share "
        "of 21% and MCP of EUR 64/MWh, compared to 26% hydro and EUR 50/MWh during "
        "wet months. The 2021-2022 drought overlapped with the Lira crisis."
    )

    h2 = load_h2()
    h2["date"] = pd.to_datetime(h2[["year", "month"]].assign(day=1))
    h2_plot = h2[(h2["year"] >= 2016) & (h2["year"] <= 2025)].copy()

    precip_bars = (
        alt.Chart(h2_plot)
        .mark_bar()
        .encode(
            x=alt.X("date:T", title="Month"),
            y=alt.Y("precip_anomaly_pct:Q", title="Precipitation Anomaly vs Long-Term Avg (%)"),
            color=alt.condition(
                alt.datum.precip_anomaly_pct > 0,
                alt.value(IBM_CAT[1]),
                alt.value(IBM_CAT[4]),
            ),
            tooltip=[
                alt.Tooltip("date:T", title="Month", format="%b %Y"),
                alt.Tooltip("precip_anomaly_pct:Q", title="Anomaly vs Average (%)", format="+.1f"),
                alt.Tooltip("precip_mm:Q", title="Actual Precipitation (mm)", format=".1f"),
                alt.Tooltip("precip_long_term_avg_mm:Q", title="Long-Term Avg (mm)", format=".1f"),
            ],
        )
        .properties(height=260)
    )
    st.altair_chart(precip_bars, use_container_width=True)
    chart_footnote(
        sources="Open-Meteo Historical Weather API (open-meteo.com). 5 stations: Artvin, Elazig, Diyarbakir, Antalya, Ankara.",
        tools="Bruin pipeline (openmeteo_turkey_weather.py), BigQuery, Altair.",
        methodology="Long-term avg = mean precipitation for that calendar month across all years (2015-2025). Anomaly = (actual - avg) / avg * 100. Blue = above average; red = below.",
        limitations="5 stations may not fully represent Turkey-wide precipitation. Station selection targets major hydro basins but misses eastern Turkey. Long-term average uses only 11 years of data.",
    )

    st.markdown("")
    st.markdown("**Monthly Hydro Share of Generation (%) and MCP Electricity Price (EUR/MWh)**")
    st.caption(
        "Hydro share is seasonal (peaks during spring snowmelt) and varies with "
        "annual precipitation. When hydro output drops, gas and coal fill the gap, "
        "pushing MCP higher. The 2021-2022 drought reduced hydro to 18% while MCP "
        "averaged EUR 141/MWh in 2022."
    )

    h2_hydro_line = (
        alt.Chart(h2_plot)
        .mark_line(strokeWidth=2, color=IBM_CAT[1])
        .encode(
            x=alt.X("date:T", title="Month"),
            y=alt.Y("hydro_share_pct:Q", title="Hydro Share of Generation (%)"),
            tooltip=[
                alt.Tooltip("date:T", title="Month", format="%b %Y"),
                alt.Tooltip("hydro_share_pct:Q", title="Hydro Share (%)", format=".1f"),
                alt.Tooltip("precip_anomaly_pct:Q", title="Precip Anomaly (%)", format="+.1f"),
            ],
        )
    )
    h2_mcp_line = (
        alt.Chart(h2_plot)
        .mark_line(strokeWidth=2, color=IBM_CAT[4], strokeDash=[6, 4])
        .encode(
            x=alt.X("date:T", title="Month"),
            y=alt.Y("mcp_avg_eur:Q", title="MCP (EUR/MWh)"),
            tooltip=[
                alt.Tooltip("date:T", title="Month", format="%b %Y"),
                alt.Tooltip("mcp_avg_eur:Q", title="MCP (EUR/MWh)", format=".2f"),
                alt.Tooltip("mcp_avg_try:Q", title="MCP (TRY/MWh)", format=",.0f"),
            ],
        )
    )

    h2_col1, h2_col2 = st.columns(2)
    with h2_col1:
        st.altair_chart(h2_hydro_line.properties(height=300), use_container_width=True)
    with h2_col2:
        st.altair_chart(h2_mcp_line.properties(height=300), use_container_width=True)

    chart_footnote(
        sources="EPIAS Transparency Platform (generation, MCP). Open-Meteo (precipitation).",
        tools="Bruin pipeline, BigQuery, Altair.",
        methodology="Hydro = dammed + run-of-river. MCP in EUR uses the EPIAS-reported EUR conversion rate. Monthly average of daily clearing prices.",
        limitations="The 2022 MCP spike reflects both drought and the Lira crisis; these effects are not isolated. MCP is influenced by many factors beyond hydro availability.",
    )

    h2_regime = h2_plot.copy()
    h2_regime["regime"] = pd.cut(
        h2_regime["precip_anomaly_pct"],
        bins=[-999, -30, -10, 10, 30, 999],
        labels=[
            "Severe Drought (<-30%)",
            "Dry (-10 to -30%)",
            "Normal (-10 to +10%)",
            "Wet (+10 to +30%)",
            "Very Wet (>+30%)",
        ],
    )
    h2_summary = (
        h2_regime.groupby("regime", observed=True)
        .agg(
            months=("year", "count"),
            hydro_share_pct=("hydro_share_pct", "mean"),
            mcp_avg_eur=("mcp_avg_eur", "mean"),
        )
        .round(2)
        .reset_index()
    )
    h2_summary.columns = ["Precipitation Regime", "Month Count", "Avg Hydro Share (%)", "Avg MCP (EUR/MWh)"]
    st.dataframe(h2_summary, hide_index=True, use_container_width=True)

    # ------------------------------------------------------------------
    # H3: Iran conflict risk and energy prices
    # ------------------------------------------------------------------
    st.markdown("---")
    st.markdown(
        "**Polymarket Iran Conflict Probability, Brent Crude Price, and Turkey MCP (Jan 2024 - Mar 2026)**"
    )
    st.caption(
        "During the 35 trading days with Polymarket Iran conflict data, higher "
        "conflict probability (>=30%) coincided with MCP averaging EUR 53/MWh vs "
        "EUR 38/MWh at low risk (<10%). Brent crude remained in the USD 68-75 range "
        "regardless. The sample size is too small for statistical conclusions."
    )

    h3 = load_h3()
    h3["date"] = pd.to_datetime(h3["date"])
    h3_with_prob = h3.dropna(subset=["iran_conflict_prob"])

    if len(h3_with_prob) > 5:
        h3_prob_line = (
            alt.Chart(h3.dropna(subset=["iran_prob_7d_avg"]))
            .mark_line(strokeWidth=2, color=IBM_CAT[4], point=alt.OverlayMarkDef(size=30))
            .encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("iran_prob_7d_avg:Q", title="Probability (0-1, 7-day avg)"),
                tooltip=[
                    alt.Tooltip("date:T", title="Date", format="%b %d, %Y"),
                    alt.Tooltip("iran_prob_7d_avg:Q", title="Iran Conflict Prob (7d avg)", format=".3f"),
                    alt.Tooltip("iran_conflict_prob:Q", title="Iran Conflict Prob (daily)", format=".3f"),
                ],
            )
            .properties(height=280)
        )
        h3_mcp_line = (
            alt.Chart(h3)
            .mark_line(strokeWidth=2, color=IBM_CAT[1])
            .encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("mcp_eur_7d_avg:Q", title="MCP EUR/MWh (7-day avg)"),
                tooltip=[
                    alt.Tooltip("date:T", title="Date", format="%b %d, %Y"),
                    alt.Tooltip("mcp_eur_7d_avg:Q", title="MCP EUR (7d avg)", format=".2f"),
                    alt.Tooltip("mcp_avg_try:Q", title="MCP TRY (daily avg)", format=",.0f"),
                    alt.Tooltip("spread_avg:Q", title="SMP-MCP Spread (TRY)", format=",.1f"),
                ],
            )
            .properties(height=280)
        )
        h3_brent_line = (
            alt.Chart(h3.dropna(subset=["brent_usd"]))
            .mark_line(strokeWidth=2, color=IBM_CAT[9])
            .encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("brent_usd:Q", title="Brent Crude (USD/barrel)"),
                tooltip=[
                    alt.Tooltip("date:T", title="Date", format="%b %d, %Y"),
                    alt.Tooltip("brent_usd:Q", title="Brent Crude (USD/bbl)", format=".2f"),
                    alt.Tooltip("nat_gas_usd:Q", title="Henry Hub Gas (USD/MMBtu)", format=".2f"),
                ],
            )
            .properties(height=280)
        )

        h3_c1, h3_c2, h3_c3 = st.columns(3)
        with h3_c1:
            st.altair_chart(h3_prob_line, use_container_width=True)
        with h3_c2:
            st.altair_chart(h3_mcp_line, use_container_width=True)
        with h3_c3:
            st.altair_chart(h3_brent_line, use_container_width=True)

        chart_footnote(
            sources="Polymarket prediction markets (Iran conflict, Yes outcome avg). EPIAS (MCP). FRED via Hormuz pipeline (Brent crude).",
            tools="Bruin pipeline, BigQuery, Altair. Cross-pipeline joins to polymarket-insights and hormuz-effect datasets.",
            methodology="Iran probability = average daily price of Yes outcomes across Iran conflict markets. MCP and Brent use 7-day rolling averages. Data starts Jan 2024 (Polymarket coverage).",
            limitations="Only 2 Iran conflict markets had price history (35 trading days overlap). Insufficient for causal inference or statistical significance. Polymarket probabilities may not reflect institutional market expectations. Brent and MCP share common drivers unrelated to geopolitical risk.",
        )
    else:
        st.info("Insufficient Iran conflict probability data for visualization (fewer than 5 trading days with data).")

    # ------------------------------------------------------------------
    # H4: Economic growth and renewable capacity
    # ------------------------------------------------------------------
    st.markdown("---")
    st.markdown("**Annual Renewable Generation by Source (TWh) and Turkey GDP per Capita PPP (2016-2024)**")
    st.caption(
        "Turkey's GDP per capita (PPP) grew from $27K to $46K (2016-2024). Over the same "
        "period, wind generation tripled from 15 to 36 TWh and solar grew from near-zero to "
        "5 TWh. Hydro fluctuates with rainfall and does not track GDP. The wind/solar growth "
        "trend coincides with economic expansion, though global technology cost reductions "
        "are also a factor."
    )

    h4 = load_h4()
    h4_plot = h4[(h4["year"] >= 2016) & (h4["year"] <= 2024)].copy()

    h4_gen = h4_plot.melt(
        id_vars=["year"],
        value_vars=["solar_twh", "wind_twh", "hydro_twh"],
        var_name="source",
        value_name="twh",
    )
    h4_gen["source"] = h4_gen["source"].map({
        "solar_twh": "Solar",
        "wind_twh": "Wind",
        "hydro_twh": "Hydro",
    })

    h4_renew_domain = ["Hydro", "Wind", "Solar"]
    h4_renew_colors = ["#1192e8", "#ee5396", "#f1c21b"]
    h4_renew_dashes = [[2, 2], [6, 4], [1, 0]]

    h4_gen_chart = (
        alt.Chart(h4_gen)
        .mark_line(strokeWidth=2.5, point=alt.OverlayMarkDef(size=50))
        .encode(
            x=alt.X("year:O", title="Year"),
            y=alt.Y("twh:Q", title="Generation (TWh)"),
            color=alt.Color(
                "source:N",
                title="Renewable Source",
                scale=alt.Scale(domain=h4_renew_domain, range=h4_renew_colors),
                legend=alt.Legend(
                    orient="top",
                    symbolType="stroke",
                    symbolSize=120,
                    symbolStrokeWidth=2.5,
                    labelFontSize=11,
                    titleFontSize=12,
                    direction="horizontal",
                ),
            ),
            strokeDash=alt.StrokeDash(
                "source:N",
                scale=alt.Scale(domain=h4_renew_domain, range=h4_renew_dashes),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("year:O", title="Year"),
                alt.Tooltip("source:N", title="Source"),
                alt.Tooltip("twh:Q", title="Generation (TWh)", format=".2f"),
            ],
        )
        .properties(height=360)
    )

    h4_gdp_chart = (
        alt.Chart(h4_plot.dropna(subset=["gdp_per_capita_ppp"]))
        .mark_bar(color="#6929c4", opacity=0.7)
        .encode(
            x=alt.X("year:O", title="Year"),
            y=alt.Y("gdp_per_capita_ppp:Q", title="GDP per Capita PPP (current intl $)"),
            tooltip=[
                alt.Tooltip("year:O", title="Year"),
                alt.Tooltip("gdp_per_capita_ppp:Q", title="GDP per Capita PPP ($)", format=",.0f"),
                alt.Tooltip("total_generation_twh:Q", title="Total Generation (TWh)", format=".1f"),
                alt.Tooltip("renewable_share_pct:Q", title="Renewable Share (%)", format=".1f"),
            ],
        )
        .properties(height=360)
    )

    h4_col1, h4_col2 = st.columns(2)
    with h4_col1:
        st.altair_chart(h4_gen_chart, use_container_width=True)
    with h4_col2:
        st.altair_chart(h4_gdp_chart, use_container_width=True)

    chart_footnote(
        sources="EPIAS Transparency Platform (generation). World Bank WDI (GDP per capita PPP, via baby-bust pipeline staging.fertility_squeeze).",
        tools="Bruin pipeline, BigQuery, Altair. Cross-pipeline join to baby-bust dataset.",
        methodology="TWh = annual MWh / 1,000,000. Hydro = dammed + river. GDP PPP = purchasing power parity in current international dollars. 2025-2026 GDP not yet available from World Bank.",
        limitations="Only 9 annual data points (2016-2024), insufficient for robust statistical correlation. GDP PPP is a broad indicator and does not isolate energy investment. Renewable growth may reflect global solar/wind cost reductions rather than domestic demand. Hydro is weather-driven. No causal claims can be made.",
    )

    h4_table = h4_plot[["year", "gdp_per_capita_ppp", "total_generation_twh",
                         "renewable_share_pct", "solar_twh", "wind_twh",
                         "hydro_twh", "coal_share_pct", "gas_share_pct"]].copy()
    h4_table["gdp_per_capita_ppp"] = h4_table["gdp_per_capita_ppp"].round(0)
    h4_table.columns = [
        "Year", "GDP PPP ($)", "Total Gen (TWh)", "Renewable Share (%)",
        "Solar (TWh)", "Wind (TWh)", "Hydro (TWh)", "Coal Share (%)", "Gas Share (%)",
    ]
    st.dataframe(h4_table, hide_index=True, use_container_width=True)


# ======================================================================
# Footer
# ======================================================================

st.markdown("---")
st.caption(
    "Data: EPIAS Transparency Platform (seffaflik.epias.com.tr) | "
    "External: FRED (exchange rates, commodities), Open-Meteo (weather), "
    "Polymarket (prediction markets), World Bank (GDP) | "
    "Pipeline: Bruin | Database: BigQuery | Visualization: Streamlit + Altair | "
    "Color palette: IBM Carbon Design System categorical (CVD-safe)"
)
