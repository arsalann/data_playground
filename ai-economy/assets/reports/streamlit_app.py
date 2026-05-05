from pathlib import Path

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(page_title="AI in the Real Economy", layout="wide")

PROJECT_ID = "bruin-playground-arsalan"
base_path = Path(__file__).parent


@st.cache_resource
def get_client():
    credentials = service_account.Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)


@st.cache_data(ttl=3600)
def run_raw(sql: str) -> pd.DataFrame:
    return get_client().query(sql).to_dataframe()


@st.cache_data(ttl=3600)
def run_query(filename: str) -> pd.DataFrame:
    sql = (base_path / filename).read_text()
    return get_client().query(sql).to_dataframe()


# ── Wong 2011 colorblind-safe palette ────────────────────────────────
VERMILLION = "#C84500"
SKY_BLUE = "#3A9AD9"
ORANGE = "#D48A00"
BLUE_GREEN = "#007A5E"
BLUE = "#0060A8"
PURPLE = "#A8507A"
DARK_GOLD = "#8A7A00"
MUTED = "#777777"
GRID = "#E5E5E5"

EXPOSURE_DOMAIN = ["Augmentation", "Hybrid", "Automation"]
EXPOSURE_RANGE = [SKY_BLUE, DARK_GOLD, VERMILLION]
EXPOSURE_SHAPES = ["circle", "square", "triangle"]

CHANNEL_DOMAIN = ["Claude.ai (consumer)", "1P API (enterprise)"]
CHANNEL_RANGE = [SKY_BLUE, VERMILLION]
CHANNEL_SHAPES = ["circle", "diamond"]

# Income-group palette: low (vermillion) → high (blue-green), with sky-blue for upper-middle.
INCOME_DOMAIN = ["Low income", "Lower-middle", "Upper-middle", "High income"]
INCOME_RANGE = [VERMILLION, ORANGE, SKY_BLUE, BLUE_GREEN]

COLLAB_DOMAIN = [
    "Directive (delegate)",
    "Task iteration",
    "Feedback loop (debug)",
    "Validation",
    "Learning",
]
COLLAB_RANGE = [VERMILLION, ORANGE, BLUE_GREEN, PURPLE, SKY_BLUE]


# Global Altair theme.
alt.themes.register("ai_economy", lambda: {
    "config": {
        "view": {"continuousWidth": 700, "continuousHeight": 480, "stroke": None},
        "legend": {
            "symbolSize": 280,
            "labelFontSize": 14,
            "titleFontSize": 14,
            "symbolStrokeWidth": 2,
            "labelLimit": 480,
            "columnPadding": 20,
            "rowPadding": 6,
            "padding": 6,
        },
        "axis": {
            "titleFontSize": 13,
            "labelFontSize": 12,
            "labelLimit": 380,
            "gridColor": GRID,
            "domainColor": MUTED,
            "tickColor": MUTED,
        },
        "title": {"fontSize": 17, "anchor": "start", "offset": 12},
    },
})
alt.themes.enable("ai_economy")


# ── Per-chart footnote helper ─────────────────────────────────────────
SOURCE_AEI = "<b>Anthropic Economic Index</b> (release 2026-01-15, CC BY 4.0)"
SOURCE_AEI_PRIOR = "<b>Anthropic Economic Index</b> (release 2025-09-15, CC BY 4.0)"
SOURCE_ONET = "<b>O*NET-SOC v29.2</b> (CC BY 4.0)"
SOURCE_BLS = "<b>BLS OES May 2024 National</b> (US public domain)"
SOURCE_WB = "<b>World Bank Open Data</b> (CC BY 4.0)"
TOOLS_LINE = "Tools: <b>Bruin</b> (orchestration) · <b>BigQuery</b> (storage) · <b>Altair</b> + <b>vega-datasets</b> (visualization)."


def footnote(sources: list[str], limits: list[str]) -> str:
    return (
        "Sources: " + " · ".join(sources) + ".<br>"
        + TOOLS_LINE + "<br>"
        + "Limitations: " + " ".join(limits)
    )


# ── Load data ─────────────────────────────────────────────────────────
chart1_df = run_query("chart1_task_exposure.sql")
chart2_df = run_query("chart2_geographic_adoption.sql")
chart3_df = run_query("chart3_consumer_vs_api.sql")
chart4_df = run_query("chart4_collaboration_dna.sql")

release_meta = run_raw("""
    SELECT
        MAX(extracted_at) AS current_extracted_at,
        MIN(date_start)   AS window_start,
        MAX(date_end)     AS window_end
    FROM `bruin-playground-arsalan.raw.aei_claude_usage`
""")
window_start = pd.to_datetime(release_meta["window_start"][0]).date()
window_end = pd.to_datetime(release_meta["window_end"][0]).date()


# ── Header ────────────────────────────────────────────────────────────
st.title("AI in the Real Economy")
st.caption(
    f"What is Claude actually used for? Each chart joins Anthropic Economic Index telemetry "
    f"({window_start:%b %-d}–{window_end:%b %-d, %Y}) against O*NET occupational tasks, BLS wages, "
    f"and World Bank country indicators. To keep the charts readable, individual O*NET task descriptions "
    f"have been rolled up to the 22 BLS major occupation groups."
)

# Top-level KPIs.
total_conv = float(chart1_df["usage_count_total"].sum())
top3_pct = float(chart1_df.head(3)["usage_count_total"].sum() / total_conv * 100)
top1 = chart1_df.iloc[0]
n_countries = int(len(chart2_df))
total_country_conv = int(chart2_df["usage_count"].sum())

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric(
        "Top occupation group's share",
        f"{top1['usage_count_total']/total_conv*100:.0f}%",
        help=f"{top1['occupation_group']} · {int(top1['usage_count_total']):,} conversations",
    )
with col2:
    st.metric("Top-3 groups capture", f"{top3_pct:.0f}% of usage")
with col3:
    st.metric("Countries (≥1M pop.)", f"{n_countries}")
with col4:
    st.metric("Country-level conversations", f"{total_country_conv:,}")

st.markdown("---")


# ══════════════════════════════════════════════════════════════════════
# CHART 1 — Concentration of AI usage across occupation groups
# ══════════════════════════════════════════════════════════════════════
df1 = chart1_df.copy()
df1 = df1.sort_values("usage_count_total", ascending=False).reset_index(drop=True)
df1["share_pct"] = df1["usage_count_total"] / df1["usage_count_total"].sum() * 100
df1["log_wage"] = np.log10(df1["median_wage_usage_weighted"])

# Correlation between usage and wage / autonomy / wage.
pearson_share_wage = float(np.corrcoef(df1["share_pct"], df1["log_wage"])[0, 1])
pearson_aut_wage = float(np.corrcoef(df1["ai_autonomy_mean"], df1["log_wage"])[0, 1])

# How concentrated is usage? Gini-like approximation via top-3 share already above.
top1 = df1.iloc[0]
bottom_share = float(df1.tail(11)["share_pct"].sum())  # bottom-half-ish

st.subheader("1 · Share of Claude conversations by occupation group")
st.caption(
    f"**Insight:** Just **3 of 22** occupation groups capture **{top3_pct:.0f}%** of all global Claude conversations — "
    f"**{top1['occupation_group']}** alone is **{top1['share_pct']:.0f}%**, more than the next two combined. "
    f"The bottom 11 groups together account for only **{bottom_share:.0f}%**. "
    f"Across groups, AI autonomy and log(wage) correlate at Pearson r = **{pearson_aut_wage:+.2f}**, "
    f"so there is no clear wage gradient — high-wage knowledge work and low-wage admin work both score in the Hybrid / Automation buckets.<br><br>"
    f"**How to read the chart:** one horizontal bar per BLS major occupation group, sorted by share of all global Claude conversations (largest first). "
    f"**Bar length (X)** = the group's share of conversations (%). "
    f"**Bar fill color** = exposure bucket — *Augmentation* (user-led, autonomy < 2.5), *Hybrid* (mixed, 2.5–3.5), or *Automation* (delegated, > 3.5). "
    f"**Right-edge label** = share %, cumulative share, usage-weighted autonomy (1–5), and US median wage. "
    f"Click an exposure bucket in the legend to isolate it.",
    unsafe_allow_html=True,
)

sel_exposure = alt.selection_point(fields=["exposure_pattern"], bind="legend")

# Cumulative share for the Pareto curve.
df1["cum_share_pct"] = df1["share_pct"].cumsum()
df1["wage_short"] = df1["median_wage_usage_weighted"].apply(lambda v: f"${v/1000:.0f}K")
df1["row_label"] = df1.apply(
    lambda r: (
        f"{r['share_pct']:.1f}%  ·  cum {r['cum_share_pct']:.0f}%  ·  "
        f"autonomy {r['ai_autonomy_mean']:.2f}  ·  wage {r['wage_short']}"
    ),
    axis=1,
)

group_order1 = df1["occupation_group"].tolist()

bars1 = alt.Chart(df1).mark_bar(
    cornerRadiusTopRight=4, cornerRadiusBottomRight=4, stroke="#222", strokeWidth=0.5,
).encode(
    y=alt.Y(
        "occupation_group:N",
        sort=group_order1,
        title=None,
        axis=alt.Axis(labelFontSize=12, labelLimit=320),
    ),
    x=alt.X(
        "share_pct:Q",
        title="Share of all global Claude conversations (%)",
        scale=alt.Scale(domain=[0, max(45.0, df1["share_pct"].max() * 1.15)]),
    ),
    color=alt.Color(
        "exposure_pattern:N",
        scale=alt.Scale(domain=EXPOSURE_DOMAIN, range=EXPOSURE_RANGE),
        title="Exposure bucket",
        legend=alt.Legend(orient="top", direction="horizontal", offset=4, columns=3),
    ),
    opacity=alt.condition(sel_exposure, alt.value(0.95), alt.value(0.18)),
    tooltip=[
        alt.Tooltip("occupation_group:N", title="Occupation group"),
        alt.Tooltip("soc_major:N", title="SOC major"),
        alt.Tooltip("share_pct:Q", title="Share of all conversations (%)", format=".2f"),
        alt.Tooltip("cum_share_pct:Q", title="Cumulative share (%)", format=".1f"),
        alt.Tooltip("usage_count_total:Q", title="Conversations", format=",.0f"),
        alt.Tooltip("ai_autonomy_mean:Q", title="AI autonomy (1-5)", format=".2f"),
        alt.Tooltip("median_wage_usage_weighted:Q", title="Usage-wt. wage", format="$,.0f"),
        alt.Tooltip("total_us_employment:Q", title="US employment", format=",.0f"),
        alt.Tooltip("occupations_observed:Q", title="Detailed occupations rolled up", format=",.0f"),
        alt.Tooltip("exposure_pattern:N", title="Exposure"),
    ],
).add_params(sel_exposure)

bar_value_labels = alt.Chart(df1).mark_text(
    align="left", baseline="middle", dx=6, fontSize=11, fontWeight=600, color="#222",
).encode(
    y=alt.Y("occupation_group:N", sort=group_order1),
    x=alt.X("share_pct:Q"),
    text="row_label:N",
)

chart1 = (bars1 + bar_value_labels).properties(height=max(560, 28 * len(df1)))

st.altair_chart(chart1, use_container_width=True)

# Companion table: every group ranked.
table1 = df1[[
    "occupation_group", "share_pct", "usage_count_total",
    "ai_autonomy_mean", "median_wage_usage_weighted", "total_us_employment", "exposure_pattern",
]].rename(columns={
    "occupation_group": "Occupation group",
    "share_pct": "Share of conversations (%)",
    "usage_count_total": "Conversations",
    "ai_autonomy_mean": "AI autonomy (1-5)",
    "median_wage_usage_weighted": "Usage-wt. median wage ($)",
    "total_us_employment": "US employment",
    "exposure_pattern": "Exposure bucket",
})
st.markdown("**All 22 occupation groups ranked by Claude conversations**")
st.dataframe(
    table1, hide_index=True,
    column_config={
        "Share of conversations (%)": st.column_config.NumberColumn(format="%.1f%%"),
        "Conversations": st.column_config.NumberColumn(format="%d"),
        "AI autonomy (1-5)": st.column_config.NumberColumn(format="%.2f"),
        "Usage-wt. median wage ($)": st.column_config.NumberColumn(format="$%,.0f"),
        "US employment": st.column_config.NumberColumn(format="%d"),
    },
    use_container_width=True,
)

st.markdown(
    f"> **{top1['occupation_group']}** is **{top1['share_pct']:.0f}%** of all global Claude usage on its own — "
    f"more than the next two groups combined ({df1.iloc[1]['occupation_group']} {df1.iloc[1]['share_pct']:.0f}% + "
    f"{df1.iloc[2]['occupation_group']} {df1.iloc[2]['share_pct']:.0f}%). "
    f"The bottom **11** groups together account for **{bottom_share:.0f}%**. "
    f"Skilled trades (Construction, Installation/Repair, Production), Transportation, and "
    f"hands-on services (Food, Cleaning, Personal Care) are nearly invisible despite making up "
    f"a large share of the US workforce. "
    f"Across groups, AI autonomy and log(wage) correlate at Pearson r = **{pearson_aut_wage:+.2f}** — "
    f"i.e. there is no strong wage gradient: high-wage knowledge work and low-wage admin work "
    f"both score in the Hybrid / Automation buckets."
)

st.caption(
    footnote(
        sources=[SOURCE_AEI, SOURCE_ONET, SOURCE_BLS],
        limits=[
            "Aggregated from individual O*NET tasks to BLS major SOC groups; metrics are weighted by AEI conversation count.",
            "BLS wages are US-only — they describe the US labor market, not the global Claude userbase.",
            "AEI is a one-week telemetry snapshot; BLS wages are an annual mean. Granularities differ intentionally.",
            "Bucket thresholds (autonomy 2.5 / 3.5) are an analyst choice for readability, not an AEI standard.",
        ],
    ),
    unsafe_allow_html=True,
)

st.divider()


# ══════════════════════════════════════════════════════════════════════
# CHART 2 — Adoption ignores GDP
# ══════════════════════════════════════════════════════════════════════
df2 = chart2_df.copy()

CENTROIDS = {
    "AFG": (33.94, 67.71), "ALB": (41.15, 20.17), "DZA": (28.03, 1.66),
    "ARG": (-38.42, -63.62), "ARM": (40.07, 45.04), "AUS": (-25.27, 133.78),
    "AUT": (47.52, 14.55), "AZE": (40.14, 47.58), "BHR": (25.93, 50.64),
    "BGD": (23.69, 90.36), "BLR": (53.71, 27.95), "BEL": (50.50, 4.47),
    "BOL": (-16.29, -63.59), "BIH": (43.92, 17.68), "BRA": (-14.24, -51.93),
    "BGR": (42.73, 25.49), "KHM": (12.57, 104.99), "CMR": (7.37, 12.35),
    "CAN": (56.13, -106.35), "CHL": (-35.68, -71.54), "CHN": (35.86, 104.20),
    "COL": (4.57, -74.30), "CRI": (9.75, -83.75), "HRV": (45.10, 15.20),
    "CUB": (21.52, -77.78), "CYP": (35.13, 33.43), "CZE": (49.82, 15.47),
    "DNK": (56.26, 9.50), "DOM": (18.74, -70.16), "ECU": (-1.83, -78.18),
    "EGY": (26.82, 30.80), "SLV": (13.79, -88.90), "EST": (58.60, 25.01),
    "ETH": (9.15, 40.49), "FIN": (61.92, 25.75), "FRA": (46.60, 1.89),
    "GEO": (42.32, 43.36), "DEU": (51.17, 10.45), "GHA": (7.95, -1.02),
    "GRC": (39.07, 21.82), "GTM": (15.78, -90.23), "HND": (15.20, -86.24),
    "HKG": (22.32, 114.17), "HUN": (47.16, 19.50), "ISL": (64.96, -19.02),
    "IND": (20.59, 78.96), "IDN": (-0.79, 113.92), "IRN": (32.43, 53.69),
    "IRQ": (33.22, 43.68), "IRL": (53.41, -8.24), "ISR": (31.05, 34.85),
    "ITA": (41.87, 12.57), "JAM": (18.11, -77.30), "JPN": (36.20, 138.25),
    "JOR": (30.59, 36.24), "KAZ": (48.02, 66.92), "KEN": (-0.02, 37.91),
    "KOR": (35.91, 127.77), "KWT": (29.31, 47.48), "KGZ": (41.20, 74.77),
    "LVA": (56.88, 24.60), "LBN": (33.85, 35.86), "LBY": (26.34, 17.23),
    "LTU": (55.17, 23.88), "LUX": (49.82, 6.13), "MYS": (4.21, 101.98),
    "MLT": (35.94, 14.38), "MEX": (23.63, -102.55), "MDA": (47.41, 28.37),
    "MNG": (46.86, 103.85), "MAR": (31.79, -7.09), "MMR": (21.92, 95.96),
    "NER": (17.61, 8.08), "NPL": (28.39, 84.12), "NLD": (52.13, 5.29),
    "NZL": (-40.90, 174.89), "NIC": (12.87, -85.21), "NGA": (9.08, 8.68),
    "MKD": (41.61, 21.75), "NOR": (60.47, 8.47), "OMN": (21.51, 55.92),
    "PAK": (30.38, 69.35), "PAN": (8.54, -80.78), "PRY": (-23.44, -58.44),
    "PER": (-9.19, -75.02), "PHL": (12.88, 121.77), "POL": (51.92, 19.15),
    "PRT": (39.40, -8.22), "PRI": (18.22, -66.59), "QAT": (25.35, 51.18),
    "ROU": (45.94, 24.97), "RUS": (61.52, 105.32), "SAU": (23.89, 45.08),
    "SRB": (44.02, 21.01), "SLE": (8.46, -11.78), "SGP": (1.35, 103.82),
    "SVK": (48.67, 19.70), "SVN": (46.15, 14.99), "ZAF": (-30.56, 22.94),
    "ESP": (40.46, -3.75), "LKA": (7.87, 80.77), "SWE": (60.13, 18.64),
    "CHE": (46.82, 8.23), "SYR": (34.80, 38.99), "TWN": (23.70, 120.96),
    "TJK": (38.86, 71.28), "THA": (15.87, 100.99), "TUN": (33.89, 9.54),
    "TTO": (10.69, -61.22), "TUR": (38.96, 35.24), "UGA": (1.37, 32.29),
    "UKR": (48.38, 31.17), "ARE": (23.42, 53.85), "GBR": (55.38, -3.44),
    "USA": (37.09, -95.71), "URY": (-32.52, -55.77), "UZB": (41.38, 64.59),
    "VEN": (6.42, -66.59), "VNM": (14.06, 108.28), "YEM": (15.55, 48.52),
    "ZMB": (-13.13, 27.85), "ZWE": (-19.02, 29.15),
}

df2["latitude"] = df2["iso_alpha_3"].map(lambda c: CENTROIDS.get(c, (None, None))[0])
df2["longitude"] = df2["iso_alpha_3"].map(lambda c: CENTROIDS.get(c, (None, None))[1])
df2 = df2.dropna(subset=["latitude", "longitude"]).copy()

# Predict expected usage per million from log(GDP) and flag the residual.
mask2 = df2["usage_per_million_people"].gt(0) & df2["gdp_per_capita"].gt(0)
sub = df2.loc[mask2].copy()
sub["log_upm"] = np.log10(sub["usage_per_million_people"])
sub["log_gdp"] = np.log10(sub["gdp_per_capita"])
slope, intercept = np.polyfit(sub["log_gdp"], sub["log_upm"], 1)
sub["log_upm_pred"] = slope * sub["log_gdp"] + intercept
sub["residual"] = sub["log_upm"] - sub["log_upm_pred"]
df2 = df2.merge(sub[["iso_alpha_3", "residual"]], on="iso_alpha_3", how="left")

corr2 = float(np.corrcoef(sub["log_upm"], sub["log_gdp"])[0, 1])
n2 = int(len(sub))

# Top over-performers and under-performers by residual.
top_over = sub.sort_values("residual", ascending=False).head(8)
top_under = sub.sort_values("residual", ascending=True).head(8)

st.subheader("2 · Per-capita Claude adoption by country, with GDP-trend residuals")
st.caption(
    f"**Insight:** Across **{n2}** countries with population ≥ 1M, log(conversations per million people) tracks "
    f"log(GDP per capita) at Pearson **r = {corr2:+.2f}** — wealth predicts adoption, but with sizable residuals. "
    f"The largest positive residuals (countries above the GDP-implied trendline) cluster around English-speaking and "
    f"former-Soviet states, suggesting that language fluency and developer-community density push adoption beyond "
    f"what income alone explains.<br><br>"
    f"**How to read the chart:** the world map below shows each country with population ≥ 1M and at least 200 observed "
    f"Claude.ai conversations. **Bubble area** = conversations per million people (sqrt scale). "
    f"**Bubble color** = World Bank income group (4 categories). "
    f"The top 8 over- and under-performers vs. the GDP-implied trendline are labeled directly on the map. "
    f"Equirectangular projection. The companion scatter below uses log scales on both axes (GDP/cap and usage/M each "
    f"span 3+ orders of magnitude); the dashed line is the OLS fit and vertical distance from it = the residual.",
    unsafe_allow_html=True,
)

# Top-15 over-performers + top-5 under-performers labeled directly.
df2["label_text"] = ""
top_over_iso = set(top_over["iso_alpha_3"])
top_under_iso = set(top_under.head(5)["iso_alpha_3"])
df2.loc[df2["iso_alpha_3"].isin(top_over_iso | top_under_iso), "label_text"] = df2["country_name"]

world_url = "https://cdn.jsdelivr.net/npm/vega-datasets@2/data/world-110m.json"
world = alt.topo_feature(world_url, "countries")

base_map = (
    alt.Chart(world)
    .mark_geoshape(fill="#F4F4F4", stroke="#FFFFFF", strokeWidth=0.5)
    .project(type="equirectangular")
    .properties(width=1500, height=620)
)

bubbles = (
    alt.Chart(df2)
    .mark_circle(opacity=0.85, stroke="#222", strokeWidth=0.7)
    .encode(
        longitude="longitude:Q",
        latitude="latitude:Q",
        size=alt.Size(
            "usage_per_million_people:Q",
            scale=alt.Scale(type="sqrt", range=[40, 2400]),
            title="Conversations per 1M people",
            legend=alt.Legend(orient="right", format=",.0f"),
        ),
        color=alt.Color(
            "income_group:N",
            scale=alt.Scale(domain=INCOME_DOMAIN, range=INCOME_RANGE),
            title="World Bank income group",
            legend=alt.Legend(orient="right"),
        ),
        tooltip=[
            alt.Tooltip("country_name:N", title="Country"),
            alt.Tooltip("iso_alpha_3:N", title="ISO"),
            alt.Tooltip("usage_per_million_people:Q", title="Conversations / 1M", format=",.0f"),
            alt.Tooltip("usage_count:Q", title="Total conversations", format=",.0f"),
            alt.Tooltip("gdp_per_capita:Q", title="GDP / capita", format="$,.0f"),
            alt.Tooltip("income_group:N", title="Income group"),
            alt.Tooltip("population:Q", title="Population", format=",.0f"),
            alt.Tooltip("residual:Q", title="Δ log10(usage/M) vs GDP trend", format="+.2f"),
            alt.Tooltip("top_task_text:N", title="Signature task"),
            alt.Tooltip("top_task_specialization_index:Q", title="Specialization × global", format=".1f"),
        ],
    )
)

country_labels = (
    alt.Chart(df2)
    .mark_text(
        align="left", baseline="middle", dx=8, dy=-8,
        fontSize=11, fontWeight=600, color="#222",
        stroke="white", strokeWidth=2.4, strokeOpacity=0.85,
    )
    .encode(longitude="longitude:Q", latitude="latitude:Q", text="label_text:N")
)

map_chart = (base_map + bubbles + country_labels).properties(width=1500, height=620).configure_view(stroke=None)
st.altair_chart(map_chart, use_container_width=True)

# Companion scatter: log(GDP) vs log(usage/M) with residual highlighting.
sub_named = sub.copy()
top_residual_set = set(top_over_iso) | set(top_under.head(8)["iso_alpha_3"])
sub_named["label_text"] = sub_named.apply(
    lambda r: r["country_name"] if r["iso_alpha_3"] in top_residual_set else "", axis=1,
)

trend_df = pd.DataFrame({
    "log_gdp": [sub["log_gdp"].min(), sub["log_gdp"].max()],
})
trend_df["log_upm"] = slope * trend_df["log_gdp"] + intercept
trend_df["gdp"] = 10 ** trend_df["log_gdp"]
trend_df["upm"] = 10 ** trend_df["log_upm"]

trend_line = alt.Chart(trend_df).mark_line(color=MUTED, strokeDash=[6, 3]).encode(
    x=alt.X("gdp:Q", scale=alt.Scale(type="log")),
    y=alt.Y("upm:Q", scale=alt.Scale(type="log")),
)

scatter = alt.Chart(sub_named).mark_circle(opacity=0.85, stroke="#222", strokeWidth=0.6).encode(
    x=alt.X(
        "gdp_per_capita:Q",
        scale=alt.Scale(type="log"),
        title="GDP per capita ($, log scale)",
        axis=alt.Axis(format="$,.0f"),
    ),
    y=alt.Y(
        "usage_per_million_people:Q",
        scale=alt.Scale(type="log"),
        title="Conversations per million people (log scale)",
    ),
    color=alt.Color(
        "income_group:N",
        scale=alt.Scale(domain=INCOME_DOMAIN, range=INCOME_RANGE),
        legend=None,
    ),
    size=alt.Size(
        "usage_count:Q",
        scale=alt.Scale(type="sqrt", range=[40, 600]),
        title="Total conversations",
        legend=alt.Legend(orient="right", format=",.0f"),
    ),
    tooltip=[
        alt.Tooltip("country_name:N", title="Country"),
        alt.Tooltip("usage_per_million_people:Q", title="Conv / 1M", format=",.0f"),
        alt.Tooltip("gdp_per_capita:Q", title="GDP / capita", format="$,.0f"),
        alt.Tooltip("residual:Q", title="Δ vs GDP trend (log10 units)", format="+.2f"),
    ],
)

scatter_labels = alt.Chart(sub_named).mark_text(
    align="left", baseline="middle", dx=7, fontSize=10, fontWeight=600, color="#222",
).encode(
    x=alt.X("gdp_per_capita:Q", scale=alt.Scale(type="log")),
    y=alt.Y("usage_per_million_people:Q", scale=alt.Scale(type="log")),
    text="label_text:N",
)

scatter_chart = (trend_line + scatter + scatter_labels).properties(height=460)
st.markdown("**Companion: GDP-vs-usage scatter with the implied trendline.**")
st.altair_chart(scatter_chart, use_container_width=True)

# Side-by-side over/under tables.
left, right = st.columns(2)
table_cols = {
    "country_name": "Country",
    "gdp_per_capita": "GDP/cap ($)",
    "usage_per_million_people": "Conv/1M",
    "residual": "Δ log10",
}
over_df = top_over[list(table_cols.keys())].rename(columns=table_cols).head(8)
under_df = top_under[list(table_cols.keys())].rename(columns=table_cols).head(8)

with left:
    st.markdown("**Top 8 over-performers vs GDP trend**")
    st.dataframe(
        over_df[["Country", "GDP/cap ($)", "Conv/1M", "Δ log10"]],
        hide_index=True,
        column_config={
            "GDP/cap ($)": st.column_config.NumberColumn(format="$%,.0f"),
            "Conv/1M": st.column_config.NumberColumn(format="%,.0f"),
            "Δ log10": st.column_config.NumberColumn(format="%+.2f"),
        },
        use_container_width=True,
    )
with right:
    st.markdown("**Top 8 under-performers vs GDP trend**")
    st.dataframe(
        under_df[["Country", "GDP/cap ($)", "Conv/1M", "Δ log10"]],
        hide_index=True,
        column_config={
            "GDP/cap ($)": st.column_config.NumberColumn(format="$%,.0f"),
            "Conv/1M": st.column_config.NumberColumn(format="%,.0f"),
            "Δ log10": st.column_config.NumberColumn(format="%+.2f"),
        },
        use_container_width=True,
    )

best_over = over_df.iloc[0]
best_under = under_df.iloc[0]
top3_pc = (
    sub.sort_values("usage_per_million_people", ascending=False)
    .head(3)["country_name"].tolist()
)
st.markdown(
    f"> Across **{n2}** countries with population ≥ 1M, log(usage per million people) tracks log(GDP per capita) "
    f"at Pearson **r = {corr2:+.2f}** — wealth predicts adoption, but with sizable residuals. "
    f"**{best_over['Country']}** uses Claude **{10**best_over['Δ log10']:.1f}× more** than its GDP would predict; "
    f"**{best_under['Country']}** uses it **{1/(10**best_under['Δ log10']):.1f}× less** than expected. "
    f"Per-capita leaders are **{top3_pc[0]}**, **{top3_pc[1]}**, and **{top3_pc[2]}** — "
    f"high-income, English-fluent, technically-skilled labor markets. "
    f"The largest positive residuals (countries above the GDP trend line) cluster around English-speaking and former-Soviet states, "
    f"suggesting that language fluency and developer-community density push adoption beyond what income alone predicts."
)

st.caption(
    footnote(
        sources=[SOURCE_AEI, SOURCE_ONET, SOURCE_WB],
        limits=[
            "Population ≥ 1M filter excludes micro-states whose tiny denominators yield extreme per-capita ratios (e.g. Nauru at 140K conv/M).",
            "AEI requires ≥200 conversations per country to publish — countries below the floor are absent.",
            "World Bank uses ISO alpha-3 country codes; AEI uses alpha-2. Staging normalizes via the AEI-published ISO mapping.",
            "GDP / capita is in current USD (NY.GDP.PCAP.CD), latest year available 2020-2024. Income-group thresholds follow the World Bank FY24 classification.",
            "AEI counts Claude.ai conversations only. Some countries may have heavy 1P API or 3P cloud usage that is not included.",
        ],
    ),
    unsafe_allow_html=True,
)

st.divider()


# ══════════════════════════════════════════════════════════════════════
# CHART 3 — Consumer (Claude.ai) vs Enterprise (1P API)
# ══════════════════════════════════════════════════════════════════════
df3 = chart3_df.copy().sort_values("delta_pp", ascending=True).reset_index(drop=True)
group_order3 = df3["occupation_group"].tolist()

most_enterprise = df3.iloc[df3["delta_pp"].idxmax()]
most_consumer = df3.iloc[df3["delta_pp"].idxmin()]

st.subheader("3 · Enterprise (1P API) vs. consumer (Claude.ai) usage gap by occupation group")
st.caption(
    f"**Insight:** the enterprise / consumer split runs along occupation lines, not task complexity. "
    f"The 1P API is **{most_enterprise['delta_pp']:+.1f}pp** more concentrated in "
    f"_{most_enterprise['occupation_group']}_ than Claude.ai is — developers using AI to write code. "
    f"Consumers use Claude **{abs(most_consumer['delta_pp']):.1f}pp** more for "
    f"_{most_consumer['occupation_group']}_ than the API does — i.e. as a learning / homework assistant.<br><br>"
    f"**How to read the chart:** each bar is one BLS major occupation group. **X-axis** = the gap between API share and "
    f"consumer share in percentage points (API share − consumer share); positive values are enterprise-skewed, negative "
    f"values are consumer-skewed. **Bar fill color** doubles the sign signal — vermillion = enterprise-heavier, "
    f"sky-blue = consumer-heavier. **Numeric label** at the end of each bar = the gap in pp.",
    unsafe_allow_html=True,
)

# Diverging bar of delta_pp, color-encoded by sign.
df3["delta_sign"] = np.where(df3["delta_pp"] >= 0, "API-heavier", "Consumer-heavier")
DELTA_DOMAIN = ["Consumer-heavier", "API-heavier"]
DELTA_RANGE = [SKY_BLUE, VERMILLION]

bars3 = alt.Chart(df3).mark_bar(stroke="#222", strokeWidth=0.5).encode(
    y=alt.Y("occupation_group:N", sort=group_order3, title=None,
            axis=alt.Axis(labelFontSize=12, labelLimit=300)),
    x=alt.X("delta_pp:Q",
            title="API share − Consumer share (percentage points)",
            scale=alt.Scale(domain=[-12, 14])),
    color=alt.Color(
        "delta_sign:N",
        scale=alt.Scale(domain=DELTA_DOMAIN, range=DELTA_RANGE),
        title="Channel skew",
        legend=alt.Legend(orient="top", direction="horizontal", offset=4),
    ),
    tooltip=[
        alt.Tooltip("occupation_group:N", title="Occupation group"),
        alt.Tooltip("consumer_pct:Q", title="Consumer share %", format=".2f"),
        alt.Tooltip("api_pct:Q", title="API share %", format=".2f"),
        alt.Tooltip("delta_pp:Q", title="Δ (API − Consumer) pp", format="+.2f"),
        alt.Tooltip("api_cost_index:Q", title="API cost index", format=".2f"),
        alt.Tooltip("api_prompt_tokens_index:Q", title="Prompt tokens index", format=".2f"),
        alt.Tooltip("api_completion_tokens_index:Q", title="Completion tokens index", format=".2f"),
        alt.Tooltip("shared_tasks:Q", title="O*NET tasks rolled up", format=",.0f"),
    ],
)

# Numeric labels on each bar.
bar_labels = alt.Chart(df3).mark_text(
    align="left", baseline="middle", dx=4, fontSize=11, fontWeight=600,
).encode(
    y=alt.Y("occupation_group:N", sort=group_order3),
    x=alt.X("delta_pp:Q"),
    text=alt.Text("delta_pp:Q", format="+.1f"),
    color=alt.value("#222"),
)

zero_line = alt.Chart(pd.DataFrame({"x": [0]})).mark_rule(color="#444", strokeWidth=1.4).encode(x="x:Q")

chart3a = (zero_line + bars3 + bar_labels).properties(height=max(560, 26 * len(df3)))
st.altair_chart(chart3a, use_container_width=True)

# Companion view: cost index vs delta to test "API skews to expensive tasks" hypothesis.
df3_cost = df3.dropna(subset=["api_cost_index"]).copy()
if len(df3_cost) >= 4:
    cost_corr = float(np.corrcoef(df3_cost["delta_pp"], df3_cost["api_cost_index"])[0, 1])
    cost_chart = (
        alt.Chart(df3_cost)
        .mark_circle(size=260, opacity=0.85, stroke="#222", strokeWidth=0.6)
        .encode(
            x=alt.X("delta_pp:Q", title="API − Consumer share (pp)"),
            y=alt.Y("api_cost_index:Q",
                    scale=alt.Scale(zero=False),
                    title="API cost index (1.0 = task average)"),
            color=alt.Color(
                "delta_sign:N",
                scale=alt.Scale(domain=DELTA_DOMAIN, range=DELTA_RANGE),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("occupation_group:N", title="Occupation group"),
                alt.Tooltip("delta_pp:Q", title="Δ pp", format="+.2f"),
                alt.Tooltip("api_cost_index:Q", title="API cost index", format=".2f"),
            ],
        )
    )
    cost_labels = (
        alt.Chart(df3_cost)
        .mark_text(align="left", baseline="middle", dx=8, fontSize=11, fontWeight=600, color="#222")
        .encode(
            x="delta_pp:Q", y="api_cost_index:Q",
            text="occupation_group:N",
        )
    )
    st.markdown(f"**Companion: API cost index vs. channel skew** (Pearson r = {cost_corr:+.2f})")
    st.caption(
        "Each dot is one occupation group. **X** = the same channel-skew gap (pp) shown above. "
        "**Y** = average API cost index (1.0 = the API task-level mean), weighted by API conversation count. "
        "Used to test whether enterprise-skewed groups are also more expensive per conversation."
    )
    st.altair_chart((cost_chart + cost_labels).properties(height=420), use_container_width=True)

mean_cost_ent = float(df3[df3["delta_pp"] > 0]["api_cost_index"].mean())
mean_cost_cons = float(df3[df3["delta_pp"] < 0]["api_cost_index"].mean())
st.markdown(
    f"> The **enterprise vs consumer split runs along occupation lines**, not task complexity. "
    f"The 1P API is **{most_enterprise['delta_pp']:+.1f}pp** more concentrated in "
    f"_{most_enterprise['occupation_group']}_ than Claude.ai is — developers are using AI to write code. "
    f"Consumers use Claude **{abs(most_consumer['delta_pp']):.1f}pp** more for "
    f"_{most_consumer['occupation_group']}_ than the API does — i.e. as a learning / homework assistant. "
    f"Mean API cost index — enterprise-skewed groups: **{mean_cost_ent:.2f}**, "
    f"consumer-skewed: **{mean_cost_cons:.2f}** "
    f"(values around 1.0 mean each conversation's cost is near the API task average; "
    f"higher = more expensive, longer prompts/completions)."
)

st.caption(
    footnote(
        sources=[SOURCE_AEI, SOURCE_ONET],
        limits=[
            "1P API data is global-only — no country breakdown is available, so the comparison is at the global × occupation-group level.",
            "Cost / token indices are normalized relative to the API task average (unitless). They are not USD.",
            "Inner join on lowercased task description drops tasks present in only one channel; an occupation group's bar reflects only the tasks observed in both.",
            "1P API ≠ all enterprise usage. Anthropic's 3P cloud distribution and large enterprise contracts are not in this feed.",
        ],
    ),
    unsafe_allow_html=True,
)

st.divider()


# ══════════════════════════════════════════════════════════════════════
# CHART 4 — Each profession has a different relationship with AI
# ══════════════════════════════════════════════════════════════════════
df4 = chart4_df.copy()

# Pivot for the headline-stat extraction.
wide4 = df4.pivot_table(
    index=["occupation_group", "soc_major", "usage_count_total"],
    columns="collaboration_type", values="share_pct", aggfunc="max",
).reset_index()
for col in COLLAB_DOMAIN:
    if col not in wide4.columns:
        wide4[col] = 0.0

wide4["dominant"] = wide4[COLLAB_DOMAIN].idxmax(axis=1)
wide4 = wide4.sort_values("usage_count_total", ascending=False).reset_index(drop=True)

learn_top = wide4.sort_values("Learning", ascending=False).iloc[0]
feedback_top = wide4.sort_values("Feedback loop (debug)", ascending=False).iloc[0]
iter_top = wide4.sort_values("Task iteration", ascending=False).iloc[0]
directive_top = wide4.sort_values("Directive (delegate)", ascending=False).iloc[0]

# Order rows by usage volume (top groups first).
group_order4 = wide4["occupation_group"].tolist()

st.subheader("4 · Collaboration-pattern mix by occupation group")
st.caption(
    f"**Insight:** there is no universal AI-usage pattern — the dominant collaboration mode is a function of the job. "
    f"**{feedback_top['occupation_group']}** has the highest **feedback-loop** share at "
    f"**{feedback_top['Feedback loop (debug)']:.0f}%** (debugging). "
    f"**{learn_top['occupation_group']}** is **{learn_top['Learning']:.0f}% learning** — practitioners using Claude "
    f"as a reference, not a delegated worker. "
    f"**{iter_top['occupation_group']}** is **{iter_top['Task iteration']:.0f}% iteration** — writers and designers "
    f"refining drafts turn by turn. "
    f"**{directive_top['occupation_group']}** is the most directive at "
    f"**{directive_top['Directive (delegate)']:.0f}%** (whole tasks handed off).<br><br>"
    f"**How to read the chart:** each horizontal bar is one BLS occupation group; the bands show the share of "
    f"conversations within that group following each collaboration pattern. "
    f"**Directive** = user dictates each step. "
    f"**Task iteration** = user refines AI output across turns. "
    f"**Feedback loop** = AI proposes, user reacts (e.g. debugging). "
    f"**Validation** = user checks AI's work. "
    f"**Learning** = user is exploring / asking AI to teach. "
    f"Bars are ordered by total Claude conversation volume; bars sum to ~100% (the small `none` and `not_classified` "
    f"slices are excluded). Click a legend entry to isolate one pattern.",
    unsafe_allow_html=True,
)

# Renormalize so each row sums to 100% across the 5 patterns.
df4 = df4.merge(
    df4.groupby("occupation_group")["share_pct"].sum().rename("group_total"),
    on="occupation_group",
)
df4["share_pct_norm"] = df4["share_pct"] / df4["group_total"] * 100

sel_collab = alt.selection_point(fields=["collaboration_type"], bind="legend")

chart4 = (
    alt.Chart(df4)
    .mark_bar(stroke="white", strokeWidth=0.5)
    .encode(
        y=alt.Y(
            "occupation_group:N", sort=group_order4, title=None,
            axis=alt.Axis(labelFontSize=12, labelLimit=320),
        ),
        x=alt.X(
            "share_pct_norm:Q",
            stack="normalize",
            title="Share of conversations on group (%, stacked to 100%)",
            axis=alt.Axis(format=".0%"),
        ),
        color=alt.Color(
            "collaboration_type:N",
            scale=alt.Scale(domain=COLLAB_DOMAIN, range=COLLAB_RANGE),
            title="Collaboration pattern",
            legend=alt.Legend(orient="top", direction="horizontal", offset=4, columns=5),
        ),
        order=alt.Order("collab_order:Q"),
        opacity=alt.condition(sel_collab, alt.value(0.95), alt.value(0.18)),
        tooltip=[
            alt.Tooltip("occupation_group:N", title="Occupation group"),
            alt.Tooltip("collaboration_type:N", title="Pattern"),
            alt.Tooltip("share_pct_norm:Q", title="Share (%)", format=".1f"),
            alt.Tooltip("usage_count_total:Q", title="Total conversations on group", format=",.0f"),
        ],
    )
    .transform_calculate(
        collab_order=(
            "datum.collaboration_type == 'Directive (delegate)' ? 0 : "
            "datum.collaboration_type == 'Task iteration' ? 1 : "
            "datum.collaboration_type == 'Feedback loop (debug)' ? 2 : "
            "datum.collaboration_type == 'Validation' ? 3 : "
            "datum.collaboration_type == 'Learning' ? 4 : 5"
        )
    )
    .add_params(sel_collab)
    .properties(height=max(560, 28 * len(group_order4)))
)
st.altair_chart(chart4, use_container_width=True)

# Companion table: dominant pattern per group.
table4 = wide4[["occupation_group", "usage_count_total"] + COLLAB_DOMAIN + ["dominant"]].rename(
    columns={
        "occupation_group": "Occupation group",
        "usage_count_total": "Conversations",
        "dominant": "Dominant pattern",
    }
)
st.markdown("**Collaboration pattern share by group (%, weighted by AEI conversation count)**")
st.dataframe(
    table4, hide_index=True,
    column_config={
        "Conversations": st.column_config.NumberColumn(format="%d"),
        **{c: st.column_config.NumberColumn(format="%.1f%%") for c in COLLAB_DOMAIN},
    },
    use_container_width=True,
)

st.markdown(
    f"> **{feedback_top['occupation_group']}** has the highest **feedback-loop** share at "
    f"**{feedback_top['Feedback loop (debug)']:.0f}%** — debugging dominates how developers use Claude. "
    f"**{learn_top['occupation_group']}** is **{learn_top['Learning']:.0f}% learning** — practitioners "
    f"in patient-facing roles are using Claude as a reference, not a delegated worker. "
    f"**{iter_top['occupation_group']}** is **{iter_top['Task iteration']:.0f}% iteration** — "
    f"writers and designers refine drafts with Claude turn by turn. "
    f"**{directive_top['occupation_group']}** is the most directive at "
    f"**{directive_top['Directive (delegate)']:.0f}%** — users hand off whole tasks. "
    f"There is no universal AI usage pattern — the dominant collaboration mode is a function of the job."
)

st.caption(
    footnote(
        sources=[SOURCE_AEI, SOURCE_ONET],
        limits=[
            "Collaboration patterns are AEI-classified per conversation; the `none` and `not_classified` slices are excluded and the remaining shares are renormalized to 100%.",
            "Patterns reflect interaction structure, not outcome quality or task success.",
            "Aggregated to BLS major SOC group; within-group variance across detailed occupations is collapsed.",
        ],
    ),
    unsafe_allow_html=True,
)

st.divider()


# ══════════════════════════════════════════════════════════════════════
# Methodology
# ══════════════════════════════════════════════════════════════════════
st.subheader("Methodology")
st.markdown(
    """
- **Primary dataset**: Anthropic Economic Index release 2026-01-15, covering a single
  one-week telemetry window (**2025-11-13 to 2025-11-20**). Each AEI release is a one-week
  snapshot — cross-release comparisons conflate time, sampling, and model version changes.
- **AEI → O*NET join**: AEI's `cluster_name` for the `onet_task` facet is the raw
  lowercased O*NET task description. We join to O*NET Task Statements on exact lowercased
  text (~84% match rate). Unmatched tasks keep AEI metrics but no occupation label.
- **SOC alignment**: O*NET-SOC uses an 8-digit extended code (`15-1252.00`); BLS OES uses
  6-digit SOC (`15-1252`). Staging truncates O*NET-SOC to the 6-digit root before joining.
  All four charts then aggregate one further level up to the **BLS major SOC group** (the
  first 2 digits — 22 groups total). This lets the charts show interpretable category
  names instead of 2,500+ task description sentences.
- **Country codes**: AEI emits ISO 3166-1 alpha-2 (`US`); World Bank uses alpha-3 (`USA`).
  Staging normalizes via the AEI-published ISO mapping (`raw.aei_iso_country_codes`).
- **Population floor**: Chart 2 keeps countries with **population ≥ 1M**, so micro-states
  whose tiny denominators yield extreme per-capita ratios (Nauru at 140K conv/M with
  ~12K residents) do not dominate.
- **Minimum-sample threshold**: countries with fewer than **200 conversations** are
  excluded from Chart 2 per the AEI publication threshold.
- **Specialization index** = country task share ÷ global task share. A value of 2 means
  the country uses Claude for that task twice as often as the global baseline. To pick a
  country's "signature task" we additionally require ≥1% of the country's conversations,
  to avoid ranking small tasks with noisy ratios.
- **`ai_autonomy_mean`** is an AEI metric on a 1-5 ordinal scale (1 = directive — user
  drives every step; 5 = fully delegated). Augmentation / Hybrid / Automation buckets
  use empirical thresholds 2.5 and 3.5; these are an analyst choice, not an AEI standard.
- **1P API data is global-only** — every chart that uses API metrics keeps the comparison
  at the global / occupation-group level.
- **BLS wages are US-only**. Chart 1 is an inherently US wage view; international wage
  enrichment at occupation granularity does not exist.
- **Model-version caveat**: the 2025-09-15 snapshot ran on Claude Sonnet 4; the 2026-01-15
  snapshot runs on Sonnet 4.5. Any release-over-release delta confounds model change with
  user behavior change.
"""
)

st.caption(
    f"As-of: AEI raw data extracted at "
    f"{pd.to_datetime(release_meta['current_extracted_at'][0]).strftime('%Y-%m-%d %H:%M UTC')}.",
)
