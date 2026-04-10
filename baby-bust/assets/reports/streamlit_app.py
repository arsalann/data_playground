from pathlib import Path

import altair as alt
import numpy as np
import pandas as pd
import requests
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(page_title="The Price Tag on the Next Generation", layout="wide")

PROJECT_ID = "bruin-playground-arsalan"


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

# Countries with population under 1 million (2022 World Bank estimates).
# To regenerate: World Bank indicator SP.POP.TOTL, filter < 1_000_000.
MICROSTATES = {
    "ABW", "AND", "ASM", "ATG", "BHS", "BLZ", "BMU", "BRB", "BRN", "BTN",
    "COM", "CPV", "CUW", "CYM", "DMA", "FJI", "FRO", "FSM", "GIB", "GRD",
    "GRL", "GUM", "GUY", "IMN", "ISL", "KIR", "KNA", "LCA", "LIE", "LUX",
    "MAC", "MCO", "MDV", "MHL", "MLT", "MNE", "MNP", "NCL", "NRU", "PLW",
    "PYF", "SLB", "SMR", "STP", "SUR", "SXM", "SYC", "TCA", "TON", "TUV",
    "VCT", "VGB", "VIR", "VUT", "WSM",
}

# SQL WHERE clause fragment for excluding microstates in run_raw() queries
_micro_list = ", ".join(f"'{c}'" for c in sorted(MICROSTATES))
MICRO_SQL = f"country_code NOT IN ({_micro_list})"

main = run_raw("""
    SELECT
        country_code, country_name, year, fertility_rate, life_expectancy,
        gdp_per_capita_ppp, urbanization_pct, female_labor_participation,
        infant_mortality, income_group, demographic_stage, above_replacement,
        region, fertility_change_5yr
    FROM `bruin-playground-arsalan.staging.fertility_squeeze`
    WHERE fertility_rate IS NOT NULL
    ORDER BY country_code, year
""")

# Filter out microstates (< 1M population)
main = main[~main["country_code"].isin(MICROSTATES)].reset_index(drop=True)

MICRO_NOTE = f"Excludes {len(MICROSTATES)} countries with population under 1 million."

# Shared data source and limitations footnote for each chart
DATA_FOOTER = (
    "Source: <b><u>World Bank Open Data API</u></b> (CC BY 4.0), indicators SP.DYN.TFRT.IN, NY.GDP.PCAP.PP.CD, "
    "SL.TLF.CACT.FE.ZS, and others. Coverage: 1960-2024, varies by indicator and country. "
    f"Excludes {len(MICROSTATES)} countries with population under 1 million and World Bank aggregate entities."
    "<br>"
    "Tools: <b><u>Bruin</u></b> (pipeline orchestration), <b><u>BigQuery</u></b> (storage/SQL), <b><u>Altair</u></b> (visualization)."
    "<br>"
    "Limitations: World Bank data may lag 1-2 years for some countries; income group classification "
    "uses latest available GNI thresholds and is applied retroactively; fertility rate is a period measure "
    "that can be affected by tempo effects (shifts in timing of births)."
)

# High-contrast colorblind-safe palette (Wong 2011, adjusted for white bg)
VERMILLION = "#C84500"  # Darker vermillion - max contrast on white
SKY_BLUE = "#3A9AD9"    # Deeper sky blue - better contrast than #56B4E9
ORANGE = "#D48A00"      # Deeper orange - distinct from vermillion
BLUE_GREEN = "#007A5E"  # Darker teal - distinct from blue
BLUE = "#0060A8"        # Deeper blue
PURPLE = "#A8507A"      # Deeper rose/mauve - distinct from all others
DARK_GOLD = "#8A7A00"   # Replaces yellow - visible on white, still warm
MUTED = "#777777"       # Darker grey for reference lines and labels

# Global Altair theme: large legends so color/shape swatches are visible
alt.themes.register("baby_bust", lambda: {
    "config": {
        "legend": {
            "symbolSize": 200,
            "labelFontSize": 13,
            "titleFontSize": 14,
            "symbolStrokeWidth": 1.5,
            "labelLimit": 250,
            "columnPadding": 16,
            "rowPadding": 6,
        },
        "axis": {
            "titleFontSize": 13,
            "labelFontSize": 12,
        },
    }
})
alt.themes.enable("baby_bust")

# ── Header ────────────────────────────────────────────────────────────

st.title("Global Fertility Trends, 1960-2024")
n_countries = main["country_code"].nunique()
st.caption(
    f"Analysis of fertility rate trends across {n_countries} countries using World Bank Open Data.  ·  "
    f"{MICRO_NOTE}  ·  "
    "Source: World Bank Open Data API (CC BY 4.0)  ·  Pipeline: Bruin + BigQuery"
)

# KPI metrics for latest year
latest_year = int(main["year"].max())
latest = main[main["year"] == latest_year]
yr_1960 = main[main["year"] == 1960]

col1, col2, col3, col4 = st.columns(4)
with col1:
    below = int(latest["above_replacement"].eq(False).sum())
    total = int(latest["fertility_rate"].notna().sum())
    st.metric("Countries Below Replacement", f"{below} of {total}",
              delta=f"{below/total*100:.0f}%", delta_color="inverse")
with col2:
    avg_now = latest["fertility_rate"].mean()
    avg_1960 = yr_1960["fertility_rate"].mean() if len(yr_1960) else None
    delta_str = f"{avg_now - avg_1960:.1f} since 1960" if avg_1960 else None
    st.metric("World Avg Fertility", f"{avg_now:.2f}", delta=delta_str, delta_color="inverse")
with col3:
    crisis_count = int((latest["demographic_stage"] == "Demographic crisis").sum())
    st.metric("Countries Below 1.5", crisis_count)
with col4:
    lowest = latest.loc[latest["fertility_rate"].idxmin()]
    st.metric("Lowest Fertility", f"{lowest['country_name']}: {lowest['fertility_rate']:.2f}")

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════
# CHART 1: The 1.5 Trap - no country that dropped below 1.5 has recovered
# Spaghetti chart of post-crisis fertility trajectories
# ══════════════════════════════════════════════════════════════════════

st.subheader("Fertility trajectories after dropping below 1.5 births per woman")
st.caption(
    "Each line shows a country's total fertility rate (TFR) from the year it first fell below 1.5. "
    "X-axis shows years elapsed since crossing that threshold. "
    "Highlighted countries: Germany (since 1975), Japan (1993), South Korea (1998), China (2019)."
)

# Compute the year each country first dropped below 1.5
crisis_entry = (
    main[main["fertility_rate"] < 1.5]
    .groupby(["country_code", "country_name"])["year"]
    .min()
    .reset_index()
    .rename(columns={"year": "crisis_year"})
)

# Build post-crisis trajectories for all countries
post_crisis = main.merge(crisis_entry, on=["country_code", "country_name"], how="inner")
post_crisis = post_crisis[post_crisis["year"] >= post_crisis["crisis_year"]].copy()
post_crisis["years_since_crisis"] = post_crisis["year"] - post_crisis["crisis_year"]

# Highlight key countries
highlight_trap = {
    "DEU": ("Germany (1975)", VERMILLION),
    "JPN": ("Japan (1993)", ORANGE),
    "KOR": ("South Korea (1998)", BLUE),
    "CHN": ("China (2019)", BLUE_GREEN),
    "ESP": ("Spain (1987)", PURPLE),
    "ITA": ("Italy (1984)", SKY_BLUE),
}

post_crisis["is_highlight"] = post_crisis["country_code"].isin(highlight_trap.keys())
post_crisis["display_name"] = post_crisis["country_code"].map(
    {k: v[0] for k, v in highlight_trap.items()}
)

# Background: all other countries as grey lines
bg_trap = post_crisis[~post_crisis["is_highlight"]]
fg_trap = post_crisis[post_crisis["is_highlight"]]

bg_lines = (
    alt.Chart(bg_trap)
    .mark_line(strokeWidth=0.8, opacity=0.15, color=MUTED)
    .encode(
        x=alt.X("years_since_crisis:Q", title="Years since dropping below 1.5",
                 scale=alt.Scale(domain=[0, 50])),
        y=alt.Y("fertility_rate:Q", title="Fertility Rate (births per woman)",
                 scale=alt.Scale(domain=[0.4, 2.5])),
        detail="country_code:N",
    )
)

trap_domain = [v[0] for v in highlight_trap.values()]
trap_range = [v[1] for v in highlight_trap.values()]

sel_trap = alt.selection_point(fields=["display_name"], bind="legend")

fg_lines = (
    alt.Chart(fg_trap)
    .mark_line(strokeWidth=3)
    .encode(
        x="years_since_crisis:Q",
        y="fertility_rate:Q",
        color=alt.Color("display_name:N", title="Country",
                        scale=alt.Scale(domain=trap_domain, range=trap_range),
                        legend=alt.Legend(orient="top", direction="horizontal")),
        strokeDash=alt.StrokeDash("display_name:N",
                                   scale=alt.Scale(domain=trap_domain,
                                                   range=[[1, 0], [12, 6], [6, 3], [2, 2], [8, 3, 2, 3], [4, 1, 4, 1]]),
                                   legend=None),
        opacity=alt.condition(sel_trap, alt.value(1), alt.value(0.15)),
        tooltip=[
            alt.Tooltip("display_name:N", title="Country"),
            alt.Tooltip("year:Q", title="Year", format="d"),
            alt.Tooltip("years_since_crisis:Q", title="Years since crisis"),
            alt.Tooltip("fertility_rate:Q", title="Fertility Rate", format=".2f"),
        ],
    )
    .properties(height=624)
    .add_params(sel_trap)
)

# Reference lines
line_1_5 = (
    alt.Chart(pd.DataFrame({"y": [1.5]}))
    .mark_rule(color=MUTED, strokeDash=[6, 4], strokeWidth=1.5)
    .encode(y="y:Q")
)
label_1_5 = (
    alt.Chart(pd.DataFrame({"y": [1.5], "x": [42], "text": ["1.5 threshold"]}))
    .mark_text(align="left", dy=-8, fontSize=11, color=MUTED)
    .encode(x="x:Q", y="y:Q", text="text:N")
)
line_2_1 = (
    alt.Chart(pd.DataFrame({"y": [2.1]}))
    .mark_rule(color=MUTED, strokeDash=[3, 3], strokeWidth=1)
    .encode(y="y:Q")
)
label_2_1 = (
    alt.Chart(pd.DataFrame({"y": [2.1], "x": [42], "text": ["Replacement (2.1)"]}))
    .mark_text(align="left", dy=-8, fontSize=11, color=MUTED)
    .encode(x="x:Q", y="y:Q", text="text:N")
)

st.altair_chart(bg_lines + fg_lines + line_1_5 + label_1_5 + line_2_1 + label_2_1, use_container_width=True)

# Compute recovery stats
recovery_stats = (
    post_crisis.groupby("country_code")
    .agg(
        crisis_year=("crisis_year", "first"),
        max_after=("fertility_rate", "max"),
        total_years=("year", "count"),
    )
    .reset_index()
)
never_recovered = int((recovery_stats["max_after"] < 1.5).sum())
barely_recovered = int(((recovery_stats["max_after"] >= 1.5) & (recovery_stats["max_after"] < 2.1)).sum())
total_crisis = len(recovery_stats)

# Countries that entered since 2019
recent_entrants = crisis_entry[crisis_entry["crisis_year"] >= 2019].sort_values("crisis_year")
recent_names = ", ".join(recent_entrants["country_name"].tolist()[:8])
recent_count = len(recent_entrants)

st.markdown(
    f"> Of **{total_crisis} countries** that have dropped below 1.5, **{never_recovered}** have not returned above 1.5, "
    f"and none have sustained a return to replacement level (2.1). "
    f"Germany has been below 1.5 for approximately 50 years; Japan for approximately 32 years. "
    f"Since 2019, **{recent_count} additional countries** entered this group, including {recent_names}."
)
st.caption(DATA_FOOTER, unsafe_allow_html=True)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════
# CHART 2: Timeline of countries entering demographic crisis (<1.5)
# Shows the acceleration - from a trickle to a flood
# ══════════════════════════════════════════════════════════════════════

st.subheader("Number of countries first falling below 1.5 TFR, by 5-year period")
st.caption(
    "Count of countries whose total fertility rate first dropped below 1.5 in each 5-year window, "
    "grouped by World Bank region. Each country appears only once, in the period it first crossed the threshold."
)

# Add region for coloring
crisis_with_region = crisis_entry.merge(
    main[["country_code", "region"]].drop_duplicates(),
    on="country_code",
    how="left",
)

# Bin into 5-year periods
crisis_with_region["period"] = (crisis_with_region["crisis_year"] // 5 * 5).astype(int)
crisis_with_region["period_label"] = crisis_with_region["period"].apply(
    lambda y: f"{y}-{y+4}"
)

# Count by period × region
period_region_counts = (
    crisis_with_region.groupby(["period", "period_label", "region"])["country_code"]
    .nunique()
    .reset_index()
    .rename(columns={"country_code": "countries"})
)

# Region ordering and colors - maximally contrasting for stacked bars
region_order_trap = [
    "Europe & Central Asia", "East Asia & Pacific", "Latin America & Caribbean",
    "North America", "Middle East & North Africa", "South Asia",
    "Sub-Saharan Africa", "Other",
]
region_colors_trap = {
    "Europe & Central Asia": "#003580",      # dark blue
    "East Asia & Pacific": "#D4B800",        # yellow
    "Latin America & Caribbean": "#C82020",  # red
    "North America": "#3A9AD9",              # bright blue
    "Middle East & North Africa": "#2E8B57", # green
    "Sub-Saharan Africa": "#2E8B57",         # green (grouped with MENA)
    "South Asia": "#D48A00",                 # orange
    "Other": "#111111",                      # black
}

sel_timeline = alt.selection_point(fields=["region"], bind="legend")

# List country names per period+region for tooltip
country_lists = (
    crisis_with_region.groupby(["period_label", "region"])["country_name"]
    .apply(lambda x: ", ".join(sorted(x.str.split(",").str[0])))
    .reset_index()
    .rename(columns={"country_name": "country_list"})
)
period_region_counts = period_region_counts.merge(country_lists, on=["period_label", "region"], how="left")

sorted_periods = sorted(period_region_counts["period_label"].unique().tolist())

timeline_bars = (
    alt.Chart(period_region_counts)
    .mark_bar(stroke="white", strokeWidth=1.5)
    .encode(
        x=alt.X("period_label:N", title="Period",
                 sort=sorted_periods,
                 axis=alt.Axis(labelAngle=-45)),
        y=alt.Y("countries:Q", title="Number of Countries", stack="zero"),
        color=alt.Color("region:N", title="Region",
                        scale=alt.Scale(
                            domain=region_order_trap,
                            range=[region_colors_trap[r] for r in region_order_trap]),
                        sort=region_order_trap,
                        legend=alt.Legend(orient="top", direction="horizontal", columns=4)),
        opacity=alt.condition(sel_timeline, alt.value(1), alt.value(0.2)),
        order=alt.Order("region_sort:Q"),
        tooltip=[
            alt.Tooltip("period_label:N", title="Period"),
            alt.Tooltip("region:N", title="Region"),
            alt.Tooltip("countries:Q", title="Countries"),
            alt.Tooltip("country_list:N", title="Names"),
        ],
    )
    .transform_calculate(
        region_sort="indexof(['Europe & Central Asia','East Asia & Pacific','Latin America & Caribbean','North America','Middle East & North Africa','South Asia','Sub-Saharan Africa','Other'], datum.region)"
    )
    .properties(height=520)
    .add_params(sel_timeline)
)

st.altair_chart(timeline_bars, use_container_width=True)

# Decade breakdown
decade_counts = crisis_with_region.copy()
decade_counts["decade"] = (decade_counts["crisis_year"] // 10 * 10).astype(int)
by_decade = decade_counts.groupby("decade").size()
d70s = int(by_decade.get(1970, 0))
d80s = int(by_decade.get(1980, 0))
d90s = int(by_decade.get(1990, 0))
d00s = int(by_decade.get(2000, 0))
d10s = int(by_decade.get(2010, 0))
d20s = int(by_decade.get(2020, 0))

st.markdown(
    f"> Countries first falling below 1.5 per decade: "
    f"**1970s**: {d70s} · **1980s**: {d80s} · **1990s**: {d90s} · "
    f"**2000s**: {d00s} · **2010s**: {d10s} · **2020s**: {d20s} (partial decade). "
    f"The 1990s had the largest concentration, largely driven by post-Soviet states in Europe & Central Asia."
)
st.caption(DATA_FOOTER, unsafe_allow_html=True)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════
# CHART 3: GDP per capita vs fertility rate scatter
# ══════════════════════════════════════════════════════════════════════

st.subheader(f"GDP per capita (PPP) vs total fertility rate, {latest_year}")
st.caption(
    f"Each point represents one country in {latest_year}. X-axis: GDP per capita at purchasing power parity "
    "(log scale, range ~$800-$143,000). Points colored and shaped by World Bank region. "
    "Dashed line indicates replacement-level fertility (2.1)."
)

scatter_data = latest[
    latest["gdp_per_capita_ppp"].notna() & latest["fertility_rate"].notna()
].copy()
scatter_data["log_gdp"] = np.log10(scatter_data["gdp_per_capita_ppp"])

highlight_countries = ["USA", "KOR", "JPN", "CHN", "IND", "NER", "NGA", "BRA", "DEU", "SGP"]
scatter_data["is_highlight"] = scatter_data["country_code"].isin(highlight_countries)
scatter_data["label"] = scatter_data.apply(
    lambda r: r["country_code"] if r["is_highlight"] else "", axis=1
)

region_order = [
    "Sub-Saharan Africa", "South Asia", "Middle East & North Africa",
    "East Asia & Pacific", "Latin America & Caribbean",
    "Europe & Central Asia", "North America"
]
region_colors = {
    "Sub-Saharan Africa": VERMILLION,
    "South Asia": DARK_GOLD,
    "Middle East & North Africa": ORANGE,
    "East Asia & Pacific": BLUE,
    "Latin America & Caribbean": PURPLE,
    "Europe & Central Asia": BLUE_GREEN,
    "North America": SKY_BLUE,
}

sel2 = alt.selection_point(fields=["region"], bind="legend")

region_shapes_scatter = {
    "Sub-Saharan Africa": "cross",
    "South Asia": "triangle-right",
    "Middle East & North Africa": "triangle-down",
    "East Asia & Pacific": "square",
    "Latin America & Caribbean": "diamond",
    "Europe & Central Asia": "circle",
    "North America": "triangle-up",
}

dots = (
    alt.Chart(scatter_data)
    .mark_point(size=70, filled=True, strokeWidth=1, stroke="white")
    .encode(
        x=alt.X("gdp_per_capita_ppp:Q", title="GDP per Capita, PPP (Log Scale)",
                 scale=alt.Scale(type="log")),
        y=alt.Y("fertility_rate:Q", title="Fertility Rate (births per woman)",
                 scale=alt.Scale(domain=[0, 8])),
        color=alt.Color("region:N", title="Region",
                        scale=alt.Scale(
                            domain=list(region_colors.keys()),
                            range=list(region_colors.values())),
                        legend=alt.Legend(orient="top", direction="horizontal",
                                         columns=4)),
        shape=alt.Shape("region:N", title="Region",
                        scale=alt.Scale(
                            domain=list(region_shapes_scatter.keys()),
                            range=list(region_shapes_scatter.values())),
                        legend=None),
        opacity=alt.condition(sel2, alt.value(0.8), alt.value(0.08)),
        tooltip=[
            alt.Tooltip("country_name:N", title="Country"),
            alt.Tooltip("fertility_rate:Q", title="Fertility Rate", format=".2f"),
            alt.Tooltip("gdp_per_capita_ppp:Q", title="GDP/capita PPP", format="$,.0f"),
            alt.Tooltip("region:N", title="Region"),
            alt.Tooltip("urbanization_pct:Q", title="Urbanization %", format=".1f"),
        ],
    )
    .properties(height=624)
    .add_params(sel2)
)

labels = (
    alt.Chart(scatter_data[scatter_data["is_highlight"]])
    .mark_text(fontSize=10, fontWeight="bold", dy=-10)
    .encode(
        x=alt.X("gdp_per_capita_ppp:Q", scale=alt.Scale(type="log")),
        y="fertility_rate:Q",
        text="label:N",
        opacity=alt.condition(sel2, alt.value(1), alt.value(0.15)),
    )
)

repl2 = (
    alt.Chart(pd.DataFrame({"y": [2.1]}))
    .mark_rule(color=MUTED, strokeDash=[6, 4], strokeWidth=1.5)
    .encode(y="y:Q")
)

st.altair_chart(dots + labels + repl2, use_container_width=True)

corr = scatter_data["fertility_rate"].corr(scatter_data["log_gdp"])
above = scatter_data[scatter_data["fertility_rate"] >= 2.1]["gdp_per_capita_ppp"]
threshold = above.median()

st.markdown(
    f"> Pearson correlation between TFR and log(GDP per capita): **r = {corr:.2f}**. "
    f"Median GDP per capita among countries still above replacement level: **${threshold:,.0f}**. "
    f"Among countries with GDP per capita above ~$15,000, few maintain replacement-level fertility."
)
st.caption("Log scale used because GDP per capita values span approximately 3 orders of magnitude. " + DATA_FOOTER, unsafe_allow_html=True)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════
# CHART 4: Average fertility rate by income group, 1960-2024
# ══════════════════════════════════════════════════════════════════════

st.subheader("Mean total fertility rate by World Bank income group, 1960-2024")
st.caption(
    "Unweighted mean of country-level TFR over time, grouped by World Bank income classification "
    "(Low, Lower-middle, Upper-middle, High). Dashed line indicates replacement level (2.1). "
    "Income group is based on latest available classification and applied to all years."
)

income_avg = (
    main[main["income_group"].notna() & main["fertility_rate"].notna()]
    .groupby(["year", "income_group"])["fertility_rate"]
    .mean()
    .reset_index()
)

income_order = ["Low", "Lower-middle", "Upper-middle", "High"]
income_colors = {
    "Low": VERMILLION,
    "Lower-middle": ORANGE,
    "Upper-middle": BLUE,
    "High": BLUE_GREEN,
}

sel1 = alt.selection_point(fields=["income_group"], bind="legend")

lines = (
    alt.Chart(income_avg)
    .mark_line(strokeWidth=3.5)
    .encode(
        x=alt.X("year:Q", title="Year", axis=alt.Axis(format="d"),
                 scale=alt.Scale(domain=[1960, 2024])),
        y=alt.Y("fertility_rate:Q", title="Fertility Rate (births per woman)",
                 scale=alt.Scale(domain=[0, 8])),
        color=alt.Color("income_group:N", title="Income Group",
                        scale=alt.Scale(domain=income_order,
                                        range=[income_colors[k] for k in income_order]),
                        legend=alt.Legend(orient="top", direction="horizontal")),
        strokeDash=alt.StrokeDash("income_group:N",
                                   scale=alt.Scale(domain=income_order,
                                                   range=[[1, 0], [12, 6], [6, 3], [2, 2]]),
                                   legend=None),
        opacity=alt.condition(sel1, alt.value(1), alt.value(0.15)),
        tooltip=[
            alt.Tooltip("income_group:N", title="Income Group"),
            alt.Tooltip("year:Q", title="Year", format="d"),
            alt.Tooltip("fertility_rate:Q", title="Fertility Rate", format=".2f"),
        ],
    )
    .properties(height=494)
    .add_params(sel1)
)

replacement_line = (
    alt.Chart(pd.DataFrame({"y": [2.1]}))
    .mark_rule(color=MUTED, strokeDash=[6, 4], strokeWidth=1.5)
    .encode(y="y:Q")
)
replacement_label = (
    alt.Chart(pd.DataFrame({"y": [2.1], "x": [1962], "text": ["Replacement level (2.1)"]}))
    .mark_text(align="left", dy=-8, fontSize=11, color=MUTED)
    .encode(x="x:Q", y="y:Q", text="text:N")
)

st.altair_chart(lines + replacement_line + replacement_label, use_container_width=True)

high_cross = income_avg[(income_avg["income_group"] == "High") & (income_avg["fertility_rate"] < 2.1)]["year"].min()
umid_cross = income_avg[(income_avg["income_group"] == "Upper-middle") & (income_avg["fertility_rate"] < 2.1)]["year"].min()

st.markdown(
    f"> In 1960, the unweighted global mean TFR was approximately **5.4**. "
    f"As of the latest data, it is **{avg_now:.1f}**. "
    f"The high-income group mean fell below replacement around **{int(high_cross)}**; "
    f"the upper-middle group around **{int(umid_cross)}**. "
    f"All four income groups show declining trends."
)
st.caption(DATA_FOOTER, unsafe_allow_html=True)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════
# CHART 5: Distribution of countries by demographic stage, 1960-2022
# ══════════════════════════════════════════════════════════════════════

st.subheader("Country distribution by demographic stage, 1960-2022")
st.caption(
    "Number of countries in each fertility stage for selected years. Stages defined by TFR thresholds: "
    "Pre-transition (>5), Early transition (3-5), Late transition (2.1-3), Below replacement (1.5-2.1), "
    "Below 1.5 (<1.5). Use the dropdown to filter by World Bank region."
)

# Region filter
all_regions = sorted(main["region"].dropna().unique().tolist())
selected_region = st.selectbox(
    "Filter by World Bank region",
    options=["All regions"] + all_regions,
    index=0,
    key="stage_region_filter",
)

decade_data = main[
    main["year"].isin([1960, 1980, 2000, 2022])
    & main["demographic_stage"].notna()
].copy()
if selected_region != "All regions":
    decade_data = decade_data[decade_data["region"] == selected_region]
decade_data["decade"] = decade_data["year"].astype(str)

stage_counts = (
    decade_data.groupby(["decade", "demographic_stage"])["country_code"]
    .nunique()
    .reset_index()
    .rename(columns={"country_code": "countries"})
)

stage_order = [
    "Pre-transition", "Early transition", "Late transition",
    "Below replacement", "Demographic crisis"
]
stage_colors = {
    "Pre-transition": VERMILLION,
    "Early transition": ORANGE,
    "Late transition": PURPLE,
    "Below replacement": SKY_BLUE,
    "Demographic crisis": BLUE,
}

bars = (
    alt.Chart(stage_counts)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, stroke="white", strokeWidth=1)
    .encode(
        x=alt.X("decade:N", title="Year",
                 sort=["1960", "1980", "2000", "2022"],
                 axis=alt.Axis(labelAngle=0)),
        y=alt.Y("countries:Q", title="Number of Countries", stack="zero"),
        color=alt.Color("demographic_stage:N", title="Demographic Stage",
                        scale=alt.Scale(domain=stage_order,
                                        range=[stage_colors[s] for s in stage_order]),
                        sort=stage_order,
                        legend=alt.Legend(orient="top", direction="horizontal",
                                         columns=5)),
        order=alt.Order("stage_sort:Q"),
        tooltip=[
            alt.Tooltip("decade:N", title="Year"),
            alt.Tooltip("demographic_stage:N", title="Stage"),
            alt.Tooltip("countries:Q", title="Countries"),
        ],
    )
    .transform_calculate(
        stage_sort="indexof(['Pre-transition','Early transition','Late transition','Below replacement','Demographic crisis'], datum.demographic_stage)"
    )
    .properties(height=494)
)

st.altair_chart(bars, use_container_width=True)

region_label = f" in {selected_region}" if selected_region != "All regions" else ""
crisis_1960 = stage_counts[(stage_counts["decade"] == "1960") & (stage_counts["demographic_stage"] == "Demographic crisis")]
crisis_2022 = stage_counts[(stage_counts["decade"] == "2022") & (stage_counts["demographic_stage"] == "Demographic crisis")]
pretrans_1960 = stage_counts[(stage_counts["decade"] == "1960") & (stage_counts["demographic_stage"] == "Pre-transition")]
pretrans_2022 = stage_counts[(stage_counts["decade"] == "2022") & (stage_counts["demographic_stage"] == "Pre-transition")]

c1960 = int(crisis_1960["countries"].values[0]) if len(crisis_1960) else 0
c2022 = int(crisis_2022["countries"].values[0]) if len(crisis_2022) else 0
p1960 = int(pretrans_1960["countries"].values[0]) if len(pretrans_1960) else 0
p2022 = int(pretrans_2022["countries"].values[0]) if len(pretrans_2022) else 0

st.markdown(
    f"> In 1960{region_label}, **{p1960} countries** had TFR above 5 and "
    f"**{c1960}** were below 1.5. "
    f"By 2022, countries with TFR above 5 decreased to **{p2022}**; countries below 1.5 increased to **{c2022}**."
)
st.caption(DATA_FOOTER, unsafe_allow_html=True)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════
# CHART 7: Ex-communist fertility - the 1990s crash and recovery
# ══════════════════════════════════════════════════════════════════════

st.subheader("Fertility trends in former communist states, 1985-2022")
st.caption(
    "Total fertility rate trajectories for 32 former communist countries, 1985-2022. "
    "The red shaded area marks 1990-2000, the period following the dissolution of the Soviet Union, "
    "during which TFR declined across most of these countries."
)

EX_SOVIET = {"KAZ", "UZB", "ARM", "KGZ", "RUS", "LVA", "GEO", "MDA", "EST", "BLR",
             "UKR", "LTU", "TJK", "TKM", "AZE"}
EX_YUGOSLAV = {"SVN", "BIH", "HRV", "SRB", "MKD"}
EASTERN_BLOC = {"BGR", "CZE", "ROU", "SVK", "HUN", "POL", "ALB"}
ASIAN_COMMUNIST = {"MNG", "CHN", "VNM", "CUB", "LAO", "KHM"}
ALL_EX_COMMUNIST = EX_SOVIET | EX_YUGOSLAV | EASTERN_BLOC | ASIAN_COMMUNIST

# Filter main data
ex_comm = main[
    main["country_code"].isin(ALL_EX_COMMUNIST) & (main["year"] >= 1985)
].copy()

ex_comm["bloc"] = ex_comm["country_code"].apply(
    lambda c: "Ex-Soviet" if c in EX_SOVIET
    else "Ex-Yugoslav" if c in EX_YUGOSLAV
    else "Eastern Bloc" if c in EASTERN_BLOC
    else "Asian communist"
)

# Bloc averages
bloc_avg = (
    ex_comm.groupby(["year", "bloc"])["fertility_rate"]
    .mean()
    .reset_index()
)

bloc_order = ["Ex-Soviet", "Ex-Yugoslav", "Eastern Bloc", "Asian communist"]
bloc_colors = {
    "Ex-Soviet": VERMILLION,
    "Ex-Yugoslav": BLUE,
    "Eastern Bloc": ORANGE,
    "Asian communist": BLUE_GREEN,
}
bloc_dashes = {
    "Ex-Soviet": [1, 0],
    "Ex-Yugoslav": [12, 6],
    "Eastern Bloc": [6, 3],
    "Asian communist": [2, 2],
}

# Top 10 highlighted countries - biggest stories in the ex-communist fertility chart
ex_comm_highlights = {
    "RUS": ("Russia",      VERMILLION),
    "KAZ": ("Kazakhstan",  PURPLE),
    "CHN": ("China",       BLUE_GREEN),
    "UKR": ("Ukraine",     SKY_BLUE),
    "POL": ("Poland",      ORANGE),
    "UZB": ("Uzbekistan",  DARK_GOLD),
    "BGR": ("Bulgaria",    BLUE),
    "CZE": ("Czechia",     "#884422"),   # Brown - extra color for 10-line chart
    "ROU": ("Romania",     "#44AA88"),   # Mint  - extra color for 10-line chart
    "ALB": ("Albania",     "#555555"),   # Charcoal
}
ex_comm["is_highlight"] = ex_comm["country_code"].isin(ex_comm_highlights.keys())
ex_comm["display_name"] = ex_comm["country_code"].map(
    {k: v[0] for k, v in ex_comm_highlights.items()}
)

# Background: all other countries as faint lines
bg_ex = ex_comm[~ex_comm["is_highlight"]]
fg_ex = ex_comm[ex_comm["is_highlight"]]

# Shared scale definitions to keep all layers aligned
x_scale_ex = alt.Scale(domain=[1985, 2023])
y_scale_ex = alt.Scale(domain=[0.5, 5])

bg_ex_lines = (
    alt.Chart(bg_ex)
    .mark_line(strokeWidth=0.7, opacity=0.15, color=MUTED)
    .encode(
        x=alt.X("year:Q", title="Year", axis=alt.Axis(format="d"), scale=x_scale_ex),
        y=alt.Y("fertility_rate:Q", title="Fertility Rate (births per woman)", scale=y_scale_ex),
        detail="country_code:N",
    )
)

hl_domain = [v[0] for v in ex_comm_highlights.values()]
hl_range = [v[1] for v in ex_comm_highlights.values()]
hl_dashes = [
    [1, 0],          # Russia - solid
    [1, 0],          # Kazakhstan - solid
    [1, 0],          # China - solid
    [12, 6],         # Ukraine - long dash
    [1, 0],          # Poland - solid
    [12, 6],         # Uzbekistan - long dash
    [6, 3],          # Bulgaria - medium dash
    [6, 3],          # Czechia - medium dash
    [2, 2],          # Romania - dotted
    [2, 2],          # Albania - dotted
]

sel_ex = alt.selection_point(fields=["display_name"], bind="legend")

fg_ex_lines = (
    alt.Chart(fg_ex)
    .mark_line(strokeWidth=3)
    .encode(
        x=alt.X("year:Q", scale=x_scale_ex),
        y=alt.Y("fertility_rate:Q", scale=y_scale_ex),
        color=alt.Color("display_name:N", title="Country",
                        scale=alt.Scale(domain=hl_domain, range=hl_range),
                        legend=alt.Legend(orient="top", direction="horizontal", columns=5)),
        strokeDash=alt.StrokeDash("display_name:N",
                                   scale=alt.Scale(domain=hl_domain, range=hl_dashes),
                                   legend=None),
        opacity=alt.condition(sel_ex, alt.value(1), alt.value(0.15)),
        tooltip=[
            alt.Tooltip("display_name:N", title="Country"),
            alt.Tooltip("year:Q", title="Year", format="d"),
            alt.Tooltip("fertility_rate:Q", title="Fertility Rate", format=".2f"),
        ],
    )
    .properties(height=624)
    .add_params(sel_ex)
)

# Collapse zone shading (1990-2000) - use year column to stay on same scale
collapse_df = pd.DataFrame({"year": [1990], "year2": [2000]})
collapse_rect = (
    alt.Chart(collapse_df)
    .mark_rect(opacity=0.08, color=VERMILLION)
    .encode(
        x=alt.X("year:Q", scale=x_scale_ex),
        x2="year2:Q",
    )
)
collapse_label = (
    alt.Chart(pd.DataFrame({"year": [1995], "fertility_rate": [4.7], "text": ["Soviet collapse"]}))
    .mark_text(fontSize=12, fontStyle="italic", color=MUTED)
    .encode(
        x=alt.X("year:Q", scale=x_scale_ex),
        y=alt.Y("fertility_rate:Q", scale=y_scale_ex),
        text="text:N",
    )
)

# Replacement level
repl_ex = (
    alt.Chart(pd.DataFrame({"fertility_rate": [2.1]}))
    .mark_rule(color=MUTED, strokeDash=[6, 4], strokeWidth=1.5)
    .encode(y=alt.Y("fertility_rate:Q", scale=y_scale_ex))
)
repl_label_ex = (
    alt.Chart(pd.DataFrame({"fertility_rate": [2.1], "year": [2019], "text": ["Replacement (2.1)"]}))
    .mark_text(align="left", dy=-8, fontSize=11, color=MUTED)
    .encode(
        x=alt.X("year:Q", scale=x_scale_ex),
        y=alt.Y("fertility_rate:Q", scale=y_scale_ex),
        text="text:N",
    )
)

st.altair_chart(
    collapse_rect + collapse_label + bg_ex_lines + fg_ex_lines + repl_ex + repl_label_ex,
    use_container_width=True,
)

# Stats
n_ex = ex_comm["country_code"].nunique()
rose_since_2000 = ex_comm[ex_comm["year"] == 2000].merge(
    ex_comm[ex_comm["year"] == 2022][["country_code", "fertility_rate"]],
    on="country_code", suffixes=("_2000", "_2022")
)
n_rose = int((rose_since_2000["fertility_rate_2022"] > rose_since_2000["fertility_rate_2000"]).sum())
n_below = int((ex_comm[ex_comm["year"] == 2022]["fertility_rate"] < 2.1).sum())

st.markdown(
    f"> Of **{n_ex} former communist countries**, **{n_rose}** recorded higher TFR in 2022 than in 2000, "
    f"while **{n_below}** remain below replacement level (2.1). "
    "The largest increases were in Kazakhstan (+1.15) and Uzbekistan (+0.70). "
    "China moved in the opposite direction, declining from 1.63 in 2000 to 1.03 in 2022."
)
st.caption(
    f"Includes {n_ex} former communist countries: {len(EX_SOVIET)} ex-Soviet, "
    f"{len(EX_YUGOSLAV)} ex-Yugoslav, {len(EASTERN_BLOC)} Eastern Bloc, "
    f"{len(ASIAN_COMMUNIST)} Asian communist. "
    "Classification is based on pre-1992 political alignment. " + DATA_FOOTER,
    unsafe_allow_html=True,
)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════
# Cross-cutting insights section
# ══════════════════════════════════════════════════════════════════════

st.subheader("Additional analysis")

# ── Female labor participation vs GDP by sub-region, sized by fertility ──
st.markdown("#### Female labor force participation vs GDP per capita by sub-region")

# UN-style sub-region mapping from ISO3 country code
SUBREGION = {
    # West Africa
    "BEN": "West Africa", "BFA": "West Africa", "CPV": "West Africa", "CIV": "West Africa",
    "GMB": "West Africa", "GHA": "West Africa", "GIN": "West Africa", "GNB": "West Africa",
    "LBR": "West Africa", "MLI": "West Africa", "MRT": "West Africa", "NER": "West Africa",
    "NGA": "West Africa", "SEN": "West Africa", "SLE": "West Africa", "TGO": "West Africa",
    # East Africa
    "BDI": "East Africa", "COM": "East Africa", "DJI": "East Africa", "ERI": "East Africa",
    "ETH": "East Africa", "KEN": "East Africa", "MDG": "East Africa", "MWI": "East Africa",
    "MUS": "East Africa", "MOZ": "East Africa", "RWA": "East Africa", "SOM": "East Africa",
    "SSD": "East Africa", "TZA": "East Africa", "UGA": "East Africa", "ZMB": "East Africa",
    "ZWE": "East Africa",
    # Central Africa
    "AGO": "Central Africa", "CMR": "Central Africa", "CAF": "Central Africa", "TCD": "Central Africa",
    "COG": "Central Africa", "COD": "Central Africa", "GNQ": "Central Africa", "GAB": "Central Africa",
    "STP": "Central Africa",
    # Southern Africa
    "BWA": "Southern Africa", "SWZ": "Southern Africa", "LSO": "Southern Africa", "NAM": "Southern Africa",
    "ZAF": "Southern Africa",
    # Western Europe
    "AUT": "Western Europe", "BEL": "Western Europe", "FRA": "Western Europe", "DEU": "Western Europe",
    "IRL": "Western Europe", "LUX": "Western Europe", "NLD": "Western Europe", "CHE": "Western Europe",
    # Northern Europe
    "DNK": "Northern Europe", "EST": "Northern Europe", "FIN": "Northern Europe", "ISL": "Northern Europe",
    "LVA": "Northern Europe", "LTU": "Northern Europe", "NOR": "Northern Europe", "SWE": "Northern Europe",
    "GBR": "Northern Europe",
    # Southern Europe
    "ALB": "Southern Europe", "BIH": "Southern Europe", "HRV": "Southern Europe", "GRC": "Southern Europe",
    "ITA": "Southern Europe", "MKD": "Southern Europe", "MNE": "Southern Europe", "PRT": "Southern Europe",
    "SRB": "Southern Europe", "SVN": "Southern Europe", "ESP": "Southern Europe", "CYP": "Southern Europe",
    "MLT": "Southern Europe",
    # Eastern Europe
    "BLR": "Eastern Europe", "BGR": "Eastern Europe", "CZE": "Eastern Europe", "HUN": "Eastern Europe",
    "MDA": "Eastern Europe", "POL": "Eastern Europe", "ROU": "Eastern Europe", "RUS": "Eastern Europe",
    "SVK": "Eastern Europe", "UKR": "Eastern Europe",
    # Central Asia
    "KAZ": "Central Asia", "KGZ": "Central Asia", "TJK": "Central Asia", "TKM": "Central Asia",
    "UZB": "Central Asia", "MNG": "Central Asia", "GEO": "Central Asia", "ARM": "Central Asia",
    "AZE": "Central Asia", "TUR": "Central Asia",
    # East Asia
    "CHN": "East Asia", "JPN": "East Asia", "KOR": "East Asia", "PRK": "East Asia", "TWN": "East Asia",
    "HKG": "East Asia", "MAC": "East Asia",
    # Southeast Asia
    "BRN": "Southeast Asia", "KHM": "Southeast Asia", "IDN": "Southeast Asia", "LAO": "Southeast Asia",
    "MYS": "Southeast Asia", "MMR": "Southeast Asia", "PHL": "Southeast Asia", "SGP": "Southeast Asia",
    "THA": "Southeast Asia", "TLS": "Southeast Asia", "VNM": "Southeast Asia",
    # Pacific Islands
    "AUS": "Pacific & Oceania", "NZL": "Pacific & Oceania", "PNG": "Pacific & Oceania",
    "FJI": "Pacific & Oceania", "SLB": "Pacific & Oceania", "VUT": "Pacific & Oceania",
    # South Asia
    "AFG": "South Asia", "BGD": "South Asia", "BTN": "South Asia", "IND": "South Asia",
    "NPL": "South Asia", "PAK": "South Asia", "LKA": "South Asia",
    # Middle East
    "BHR": "Middle East", "IRQ": "Middle East", "ISR": "Middle East", "JOR": "Middle East",
    "KWT": "Middle East", "LBN": "Middle East", "OMN": "Middle East", "QAT": "Middle East",
    "SAU": "Middle East", "ARE": "Middle East", "YEM": "Middle East", "IRN": "Middle East",
    "PSE": "Middle East", "SYR": "Middle East",
    # North Africa
    "DZA": "North Africa", "EGY": "North Africa", "LBY": "North Africa", "MAR": "North Africa",
    "TUN": "North Africa",
    # South America
    "ARG": "South America", "BOL": "South America", "BRA": "South America", "CHL": "South America",
    "COL": "South America", "ECU": "South America", "GUY": "South America", "PRY": "South America",
    "PER": "South America", "SUR": "South America", "URY": "South America", "VEN": "South America",
    # Central America & Caribbean
    "BLZ": "Central America & Caribbean", "CRI": "Central America & Caribbean",
    "SLV": "Central America & Caribbean", "GTM": "Central America & Caribbean",
    "HND": "Central America & Caribbean", "MEX": "Central America & Caribbean",
    "NIC": "Central America & Caribbean", "PAN": "Central America & Caribbean",
    "CUB": "Central America & Caribbean", "DOM": "Central America & Caribbean",
    "HTI": "Central America & Caribbean", "JAM": "Central America & Caribbean",
    "TTO": "Central America & Caribbean",
    # North America
    "CAN": "North America", "USA": "North America",
}

# Parent region for coloring sub-regions
SUBREGION_PARENT = {
    "West Africa": "Sub-Saharan Africa", "East Africa": "Sub-Saharan Africa",
    "Central Africa": "Sub-Saharan Africa", "Southern Africa": "Sub-Saharan Africa",
    "Western Europe": "Europe & Central Asia", "Northern Europe": "Europe & Central Asia",
    "Southern Europe": "Europe & Central Asia", "Eastern Europe": "Europe & Central Asia",
    "Central Asia": "Europe & Central Asia",
    "East Asia": "East Asia & Pacific", "Southeast Asia": "East Asia & Pacific",
    "Pacific & Oceania": "East Asia & Pacific",
    "South Asia": "South Asia",
    "Middle East": "Middle East & North Africa", "North Africa": "Middle East & North Africa",
    "South America": "Latin America & Caribbean",
    "Central America & Caribbean": "Latin America & Caribbean",
    "North America": "North America",
}

# Fetch country-level data, latest available year per country
bubble_data = run_raw(f"""
    WITH ranked AS (
        SELECT country_name, country_code, region, year,
               ROUND(fertility_rate, 2) as fertility_rate,
               ROUND(female_labor_participation, 1) as female_labor_pct,
               ROUND(gdp_per_capita_ppp, 0) as gdp_ppp
        FROM `bruin-playground-arsalan.staging.fertility_squeeze`
        WHERE female_labor_participation IS NOT NULL
          AND fertility_rate IS NOT NULL
          AND gdp_per_capita_ppp IS NOT NULL
          AND {MICRO_SQL}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY country_code ORDER BY year DESC) = 1
    )
    SELECT * FROM ranked
""")

# Fetch population from World Bank API for population-weighted averages
@st.cache_data(ttl=86400)
def fetch_wb_population():
    """Fetch latest population by country from World Bank API."""
    url = (
        "https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL"
        "?format=json&per_page=1000&date=2023"
    )
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if len(data) < 2:
            return {}
        result = {}
        for rec in data[1]:
            iso3 = rec.get("countryiso3code", "")
            val = rec.get("value")
            if iso3 and val is not None:
                result[iso3] = val
        return result
    except Exception:
        return {}

pop_map = fetch_wb_population()
bubble_data["population"] = bubble_data["country_code"].map(pop_map)

# Map to sub-region and aggregate (population-weighted)
bubble_data["subregion"] = bubble_data["country_code"].map(SUBREGION)
bubble_data["parent_region"] = bubble_data["subregion"].map(SUBREGION_PARENT)
bubble_data = bubble_data.dropna(subset=["subregion"])

# Fill missing population with 0 so they don't break weighted avg (they'll be excluded)
bubble_data["pop_weight"] = bubble_data["population"].fillna(0)

def weighted_avg(group):
    w = group["pop_weight"].values
    total_w = w.sum()
    if total_w == 0:
        # Fallback to simple mean if no population data
        return pd.Series({
            "fertility_rate": group["fertility_rate"].mean(),
            "female_labor_pct": group["female_labor_pct"].mean(),
            "gdp_ppp": group["gdp_ppp"].mean(),
            "n_countries": group["country_code"].nunique(),
            "total_pop_m": 0,
        })
    return pd.Series({
        "fertility_rate": np.average(group["fertility_rate"], weights=w),
        "female_labor_pct": np.average(group["female_labor_pct"], weights=w),
        "gdp_ppp": np.average(group["gdp_ppp"], weights=w),
        "n_countries": group["country_code"].nunique(),
        "total_pop_m": total_w / 1e6,
    })

subregion_agg = (
    bubble_data.groupby(["subregion", "parent_region"])
    .apply(weighted_avg, include_groups=False)
    .reset_index()
)
subregion_agg["fertility_rate"] = subregion_agg["fertility_rate"].round(2)
subregion_agg["female_labor_pct"] = subregion_agg["female_labor_pct"].round(1)
subregion_agg["gdp_ppp"] = subregion_agg["gdp_ppp"].round(0)
subregion_agg["total_pop_m"] = subregion_agg["total_pop_m"].round(1)

st.caption(
    "Each bubble represents one sub-region, showing the population-weighted average of its constituent countries. "
    "X-axis: female labor force participation rate (%). Y-axis: GDP per capita at PPP ($). "
    "Bubble size encodes average TFR. Color indicates parent World Bank region. "
    "Population weights from World Bank indicator SP.POP.TOTL (most recent available year)."
)

sel_bubble = alt.selection_point(fields=["parent_region"], bind="legend")

# Auto-scale y-axis with padding
y_max = int(subregion_agg["gdp_ppp"].max() * 1.15)

bubble_dots = (
    alt.Chart(subregion_agg)
    .mark_circle(strokeWidth=1.5, stroke="white", opacity=0.85)
    .encode(
        x=alt.X("female_labor_pct:Q", title="Female Labor Force Participation (%)",
                 scale=alt.Scale(domain=[15, 80])),
        y=alt.Y("gdp_ppp:Q", title="GDP per Capita, PPP ($)",
                 scale=alt.Scale(domain=[0, y_max])),
        size=alt.Size("fertility_rate:Q", title="Avg Fertility Rate",
                       scale=alt.Scale(domain=[1, 5.5], range=[80, 1200]),
                       legend=alt.Legend(orient="right", direction="vertical",
                                         values=[1, 2, 3, 4, 5])),
        color=alt.Color("parent_region:N", title="Region",
                        scale=alt.Scale(
                            domain=list(region_colors.keys()),
                            range=list(region_colors.values())),
                        legend=alt.Legend(orient="top", direction="horizontal",
                                         columns=4)),
        opacity=alt.condition(sel_bubble, alt.value(0.85), alt.value(0.08)),
        tooltip=[
            alt.Tooltip("subregion:N", title="Sub-region"),
            alt.Tooltip("parent_region:N", title="Region"),
            alt.Tooltip("n_countries:Q", title="Countries"),
            alt.Tooltip("total_pop_m:Q", title="Population (millions)", format=",.1f"),
            alt.Tooltip("female_labor_pct:Q", title="Wtd Female Labor %", format=".1f"),
            alt.Tooltip("gdp_ppp:Q", title="Wtd GDP/capita PPP", format="$,.0f"),
            alt.Tooltip("fertility_rate:Q", title="Wtd Fertility Rate", format=".2f"),
        ],
    )
    .properties(height=624)
    .add_params(sel_bubble)
)

bubble_labels = (
    alt.Chart(subregion_agg)
    .mark_text(fontSize=11, fontWeight="bold", dy=-14)
    .encode(
        x="female_labor_pct:Q",
        y="gdp_ppp:Q",
        text="subregion:N",
        opacity=alt.condition(sel_bubble, alt.value(1), alt.value(0.1)),
    )
)

st.altair_chart(bubble_dots + bubble_labels, use_container_width=True)

# Stats
ssa_subs = subregion_agg[subregion_agg["parent_region"] == "Sub-Saharan Africa"]
eur_subs = subregion_agg[subregion_agg["parent_region"] == "Europe & Central Asia"]
ssa_avg_labor = ssa_subs["female_labor_pct"].mean()
ssa_avg_fert = ssa_subs["fertility_rate"].mean()
eur_avg_labor = eur_subs["female_labor_pct"].mean()
eur_avg_fert = eur_subs["fertility_rate"].mean()

st.markdown(
    f"> Population-weighted averages: Sub-Saharan African sub-regions average **{ssa_avg_labor:.0f}% female labor participation** "
    f"with a mean TFR of **{ssa_avg_fert:.1f}**, compared to European sub-regions at "
    f"**{eur_avg_labor:.0f}%** participation and TFR of **{eur_avg_fert:.1f}**. "
    "Note: the World Bank female labor participation metric includes both formal and informal employment, "
    "including subsistence agriculture, which may limit comparability across regions."
)
st.caption(DATA_FOOTER, unsafe_allow_html=True)

# ── Fastest vs slowest crashes ──
st.markdown("#### Largest 5-year declines in total fertility rate")
st.caption(
    "Top 10 countries by magnitude of 5-year TFR decline. Each country appears once (its largest single decline). "
    "Color distinguishes declines associated with conflict or state policy from those occurring during "
    "voluntary demographic transitions."
)

fast_crashes = run_raw(f"""
    SELECT country_name,
           year,
           ROUND(fertility_rate, 2) as fertility,
           ROUND(fertility_change_5yr, 2) as five_yr_change
    FROM `bruin-playground-arsalan.staging.fertility_squeeze`
    WHERE fertility_change_5yr IS NOT NULL
      AND {MICRO_SQL}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country_code ORDER BY fertility_change_5yr ASC) = 1
    ORDER BY fertility_change_5yr ASC
    LIMIT 10
""")
fast_crashes["bar_label"] = fast_crashes["country_name"].str.split(",").str[0] + " (" + fast_crashes["year"].astype(str) + ")"
coerced = ["Cambodia", "China"]
fast_crashes["context"] = fast_crashes["country_name"].apply(
    lambda n: "Conflict or coercion" if any(c in n for c in coerced) else "Voluntary transition"
)

crash_bars = (
    alt.Chart(fast_crashes)
    .mark_bar(cornerRadiusEnd=3, stroke="#333", strokeWidth=0.5)
    .encode(
        x=alt.X("five_yr_change:Q", title="5-Year Fertility Change (births per woman)"),
        y=alt.Y("bar_label:N", title="", sort=alt.EncodingSortField(field="five_yr_change", order="ascending")),
        color=alt.Color("context:N", title="",
                        scale=alt.Scale(domain=["Conflict or coercion", "Voluntary transition"],
                                        range=[VERMILLION, BLUE]),
                        legend=alt.Legend(orient="top", direction="horizontal")),
        tooltip=[
            alt.Tooltip("country_name:N", title="Country"),
            alt.Tooltip("year:Q", title="Year", format="d"),
            alt.Tooltip("fertility:Q", title="Fertility that year", format=".2f"),
            alt.Tooltip("five_yr_change:Q", title="5-Year Change", format="+.2f"),
        ],
    )
    .properties(height=max(len(fast_crashes) * 39, 260))
)

crash_val_labels = (
    alt.Chart(fast_crashes)
    .mark_text(align="right", dx=-5, fontSize=11, color="white", fontWeight="bold")
    .encode(
        x="five_yr_change:Q",
        y=alt.Y("bar_label:N", sort=alt.EncodingSortField(field="five_yr_change", order="ascending")),
        text=alt.Text("five_yr_change:Q", format="+.2f"),
    )
)

st.altair_chart(crash_bars + crash_val_labels, use_container_width=True)
st.markdown(
    "> Cambodia's 1977 decline (-3.23 over 5 years) occurred during the Khmer Rouge period. "
    "China's 1975 decline (-2.51) coincided with the implementation of family planning policies. "
    "Iran's mid-1990s decline (-2.08) is among the largest not associated with conflict or coercive policy."
)
st.caption(DATA_FOOTER, unsafe_allow_html=True)

# ── Countries where fertility ROSE ──
st.markdown("#### Countries with higher TFR in latest data than in 2000")
st.caption(
    "Countries where the most recent total fertility rate exceeds the 2000 value. "
    "Each pair of points shows the 2000 TFR (circle) and latest available TFR (diamond), connected by a line."
)

reversals = run_raw(f"""
    WITH yr2000 AS (
        SELECT country_code, country_name, fertility_rate as f2000
        FROM `bruin-playground-arsalan.staging.fertility_squeeze`
        WHERE year = 2000 AND fertility_rate IS NOT NULL AND {MICRO_SQL}
    ),
    yr_latest AS (
        SELECT country_code, fertility_rate as f_latest
        FROM `bruin-playground-arsalan.staging.fertility_squeeze`
        WHERE year = {latest_year} AND fertility_rate IS NOT NULL AND {MICRO_SQL}
    )
    SELECT a.country_name,
           a.country_code,
           ROUND(a.f2000, 2) as f2000,
           ROUND(b.f_latest, 2) as f_latest,
           ROUND(b.f_latest - a.f2000, 2) as change
    FROM yr2000 a JOIN yr_latest b ON a.country_code = b.country_code
    WHERE b.f_latest > a.f2000
    ORDER BY b.f_latest - a.f2000 DESC
""")
if len(reversals):
    reversals["country_short"] = reversals["country_name"].str.split(",").str[0]
    # Melt to long form for dumbbell chart
    rev_long = pd.melt(
        reversals, id_vars=["country_short", "change"],
        value_vars=["f2000", "f_latest"],
        var_name="period", value_name="fertility"
    )
    rev_long["period"] = rev_long["period"].map({"f2000": "2000", "f_latest": str(latest_year)})

    # Connecting lines
    rev_lines = (
        alt.Chart(rev_long)
        .mark_line(color=MUTED, strokeWidth=2.7)
        .encode(
            x=alt.X("fertility:Q", title="Fertility Rate (births per woman)"),
            y=alt.Y("country_short:N", title="",
                     sort=alt.EncodingSortField(field="change", order="descending")),
            detail="country_short:N",
        )
    )

    # Dots for each year
    rev_dots = (
        alt.Chart(rev_long)
        .mark_point(size=122, filled=True, strokeWidth=1.5, stroke="white")
        .encode(
            x="fertility:Q",
            y=alt.Y("country_short:N",
                     sort=alt.EncodingSortField(field="change", order="descending")),
            color=alt.Color("period:N", title="",
                            scale=alt.Scale(domain=["2000", str(latest_year)],
                                            range=[BLUE, VERMILLION]),
                            legend=alt.Legend(orient="top", direction="horizontal")),
            shape=alt.Shape("period:N", title="",
                            scale=alt.Scale(domain=["2000", str(latest_year)],
                                            range=["circle", "diamond"]),
                            legend=None),
            tooltip=[
                alt.Tooltip("country_short:N", title="Country"),
                alt.Tooltip("period:N", title="Year"),
                alt.Tooltip("fertility:Q", title="Fertility Rate", format=".2f"),
            ],
        )
    )

    rev_chart = (rev_lines + rev_dots).properties(height=max(len(reversals) * 36, 260))
    st.altair_chart(rev_chart, use_container_width=True)
    st.markdown(
        "> The majority of countries with TFR increases since 2000 are former Soviet states "
        "where fertility rebounded from 1990s lows (e.g., Kazakhstan, Armenia, Georgia, Romania). "
        "Central African Republic is an exception: its TFR rose from 5.85 to 6.02 despite already "
        "being among the highest globally."
    )
    st.caption(DATA_FOOTER, unsafe_allow_html=True)

# ── Sub-Saharan fastest decliners ──
st.markdown("#### Largest TFR declines in Sub-Saharan Africa, 2000-2022")
st.caption(
    "Top 10 Sub-Saharan African countries by magnitude of TFR decline between 2000 and 2022. "
    "Circle = 2000 value, diamond = 2022 value. Dashed vertical line marks TFR of 1.5."
)

ssa_fast = run_raw(f"""
    WITH yr2000 AS (
        SELECT country_code, country_name, fertility_rate as f2000
        FROM `bruin-playground-arsalan.staging.fertility_squeeze`
        WHERE year = 2000 AND region = 'Sub-Saharan Africa' AND fertility_rate IS NOT NULL AND {MICRO_SQL}
    ),
    yr2022 AS (
        SELECT country_code, fertility_rate as f2022
        FROM `bruin-playground-arsalan.staging.fertility_squeeze`
        WHERE year = 2022 AND fertility_rate IS NOT NULL AND {MICRO_SQL}
    )
    SELECT a.country_name,
           ROUND(a.f2000, 2) as f2000,
           ROUND(b.f2022, 2) as f2022,
           ROUND(b.f2022 - a.f2000, 2) as change
    FROM yr2000 a JOIN yr2022 b ON a.country_code = b.country_code
    ORDER BY b.f2022 - a.f2000 ASC
    LIMIT 10
""")
ssa_fast["country_short"] = ssa_fast["country_name"].str.split(",").str[0]

ssa_long = pd.melt(
    ssa_fast, id_vars=["country_short", "change"],
    value_vars=["f2000", "f2022"],
    var_name="period", value_name="fertility"
)
ssa_long["period"] = ssa_long["period"].map({"f2000": "2000", "f2022": "2022"})

ssa_lines = (
    alt.Chart(ssa_long)
    .mark_line(color=MUTED, strokeWidth=2)
    .encode(
        x=alt.X("fertility:Q", title="Fertility Rate (births per woman)"),
        y=alt.Y("country_short:N", title="",
                 sort=alt.EncodingSortField(field="change", order="ascending")),
        detail="country_short:N",
    )
)

ssa_dots = (
    alt.Chart(ssa_long)
    .mark_point(size=90, filled=True, strokeWidth=1.5, stroke="white")
    .encode(
        x="fertility:Q",
        y=alt.Y("country_short:N",
                 sort=alt.EncodingSortField(field="change", order="ascending")),
        color=alt.Color("period:N", title="",
                        scale=alt.Scale(domain=["2000", "2022"],
                                        range=[VERMILLION, BLUE_GREEN]),
                        legend=alt.Legend(orient="top", direction="horizontal")),
        shape=alt.Shape("period:N", title="",
                        scale=alt.Scale(domain=["2000", "2022"],
                                        range=["circle", "diamond"]),
                        legend=None),
        tooltip=[
            alt.Tooltip("country_short:N", title="Country"),
            alt.Tooltip("period:N", title="Year"),
            alt.Tooltip("fertility:Q", title="Fertility Rate", format=".2f"),
        ],
    )
)

# 1.5 crisis threshold
ssa_crisis_line = (
    alt.Chart(pd.DataFrame({"x": [1.5]}))
    .mark_rule(color=VERMILLION, strokeDash=[6, 4], strokeWidth=1)
    .encode(x="x:Q")
)

ssa_chart = (ssa_lines + ssa_dots + ssa_crisis_line).properties(height=max(len(ssa_fast) * 39, 260))
st.altair_chart(ssa_chart, use_container_width=True)
st.markdown(
    "> Cabo Verde recorded the largest decline in this group, from 3.55 to 1.53, placing it below 1.5. "
    "Ethiopia (-2.57), Sierra Leone (-2.48), and Rwanda (-2.19) also recorded substantial declines. "
    "These figures represent the 10 largest declines within the Sub-Saharan Africa region."
)
st.caption(DATA_FOOTER, unsafe_allow_html=True)

st.markdown("---")

# ── Footer ────────────────────────────────────────────────────────────

st.markdown(
    f"**Methodology**: All data sourced from the <b><u>World Bank Open Data API</u></b> (CC BY 4.0). "
    f"Analysis covers {n_countries} countries after excluding World Bank aggregate entities "
    f"and {len(MICROSTATES)} countries with population under 1 million. "
    "Income groups use World Bank GNI per capita thresholds applied retroactively using the latest classification. "
    "Demographic stages are defined by total fertility rate (TFR) thresholds: "
    "Pre-transition (>5), Early transition (3-5), Late transition (2.1-3), Below replacement (1.5-2.1), "
    "Below 1.5 (<1.5). The replacement-level threshold of 2.1 is a standard approximation for countries "
    "with low child mortality. "
    "TFR is a period measure and can be affected by tempo effects (shifts in timing of births). "
    "World Bank data may lag 1-2 years for some countries and indicators. "
    "Population-weighted averages use World Bank indicator SP.POP.TOTL (most recent available year).",
    unsafe_allow_html=True,
)
st.caption(
    "Visualization: Wong 2011 colorblind-safe palette, <b><u>Altair</u></b>. "
    "Pipeline: <b><u>Bruin</u></b> (orchestration), <b><u>BigQuery</u></b> (storage and transformation). "
    "Data: <b><u>World Bank Open Data API</u></b>, 1960-2024.",
    unsafe_allow_html=True,
)
