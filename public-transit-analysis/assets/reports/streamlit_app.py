from pathlib import Path

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(page_title="US Public Transit: Recovery & Efficiency", layout="wide")

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


# ── IBM Design Language - Colorblind Safe Palette ───────────────────
# Source: IBM Design for Accessibility data visualization guidelines
# 5-color categorical: Blue, Purple, Magenta, Orange, Gold
IBM_BLUE = "#648FFF"
IBM_PURPLE = "#785EF0"
IBM_MAGENTA = "#DC267F"
IBM_ORANGE = "#FE6100"
IBM_YELLOW = "#FFB000"
# Extended: status and supplementary from IBM Carbon Design System
IBM_GREEN = "#198038"
IBM_RED = "#DA1E28"
IBM_TEAL = "#009D9A"
IBM_CYAN = "#1192E8"
IBM_GREY = "#6F6F6F"
IBM_DARK = "#161616"

MODE_COLORS = {
    "Bus": IBM_BLUE,
    "Heavy Rail": IBM_ORANGE,
    "Light Rail": IBM_YELLOW,
    "Commuter Rail": IBM_PURPLE,
    "Bus Rapid Transit": IBM_TEAL,
    "Commuter Bus": IBM_MAGENTA,
    "Ferryboat": IBM_GREY,
    "Demand Response": IBM_CYAN,
}

# Threshold labels and colors for bar charts
THRESH_LABELS = ["Above baseline", "Partial recovery", "Below target"]
THRESH_COLORS = [IBM_GREEN, IBM_BLUE, IBM_RED]


# ── Global Altair Theme (IBM-inspired) ─────────────────────────────
def ibm_theme():
    return {
        "config": {
            "axis": {
                "labelFontSize": 13,
                "titleFontSize": 15,
                "titleFontWeight": "normal",
                "labelColor": "#525252",
                "titleColor": "#161616",
                "gridColor": "#e0e0e0",
            },
            "legend": {
                "labelFontSize": 13,
                "titleFontSize": 14,
                "titleColor": "#161616",
                "labelColor": "#525252",
                "symbolSize": 140,
                "symbolStrokeWidth": 0,
            },
            "title": {
                "fontSize": 16,
                "color": "#161616",
            },
            "text": {
                "fontSize": 12,
                "color": "#161616",
            },
            "view": {
                "strokeWidth": 0,
            },
        }
    }


alt.themes.register("ibm", ibm_theme)
alt.themes.enable("ibm")


# ── Load Data ────────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def load_mode_recovery():
    """National ridership by mode, annually, indexed to 2019."""
    return run_raw("""
        WITH national AS (
            SELECT
                CASE
                    WHEN mode IN ('HR') THEN 'Heavy Rail'
                    WHEN mode IN ('LR') THEN 'Light Rail'
                    WHEN mode IN ('CR') THEN 'Commuter Rail'
                    WHEN mode IN ('MB') THEN 'Bus'
                    WHEN mode IN ('RB') THEN 'Bus Rapid Transit'
                    WHEN mode IN ('CB') THEN 'Commuter Bus'
                END AS mode_name,
                EXTRACT(YEAR FROM report_month) AS report_year,
                SUM(upt) AS total_upt
            FROM `bruin-playground-arsalan.staging.transit_ridership_recovery`
            WHERE mode IN ('HR', 'LR', 'CR', 'MB', 'RB', 'CB')
            GROUP BY 1, 2
        ),
        baseline AS (
            SELECT mode_name, total_upt AS baseline_upt
            FROM national WHERE report_year = 2019
        )
        SELECT
            n.mode_name,
            n.report_year,
            n.total_upt,
            ROUND(SAFE_DIVIDE(CAST(n.total_upt AS FLOAT64),
                              CAST(b.baseline_upt AS FLOAT64)) * 100, 1) AS recovery_pct
        FROM national n
        LEFT JOIN baseline b ON n.mode_name = b.mode_name
        WHERE n.report_year BETWEEN 2002 AND 2025
        ORDER BY n.mode_name, n.report_year
    """)


@st.cache_data(ttl=3600)
def load_metro_recovery():
    """Metro-level recovery with WFH rates."""
    return run_raw("""
        SELECT
            uza_name,
            report_year,
            uza_population,
            total_upt,
            total_operating_expenses,
            metro_cost_per_trip,
            metro_fare_recovery,
            trips_per_capita,
            expense_per_capita,
            msa_name,
            transit_mode_share_pct,
            wfh_rate_pct,
            ridership_2019,
            recovery_pct,
            num_agencies
        FROM `bruin-playground-arsalan.staging.transit_metro_comparison`
        WHERE report_year = 2023
          AND ridership_2019 > 500000
          AND recovery_pct IS NOT NULL
          AND uza_name NOT LIKE '%Non-UZA%'
        ORDER BY total_upt DESC
    """)


@st.cache_data(ttl=3600)
def load_agency_efficiency():
    """Agency-level efficiency for 2024."""
    return run_raw("""
        SELECT
            agency,
            state,
            uza_name,
            primary_uza_population,
            total_upt,
            total_operating_expenses,
            total_fare_revenue,
            fare_recovery_ratio,
            cost_per_trip,
            trips_per_capita,
            expense_per_capita,
            primary_mode
        FROM `bruin-playground-arsalan.staging.transit_agency_efficiency`
        WHERE report_year = 2024
          AND total_upt > 1000000
        ORDER BY total_upt DESC
    """)


@st.cache_data(ttl=3600)
def load_annual_totals():
    """National annual totals from monthly data."""
    return run_raw("""
        SELECT
            report_year,
            SUM(upt) AS total_upt,
            COUNT(DISTINCT ntd_id) AS agencies
        FROM `bruin-playground-arsalan.staging.transit_ridership_recovery`
        WHERE report_year BETWEEN 2017 AND 2025
        GROUP BY report_year
        ORDER BY report_year
    """)


# ── Helper: assign threshold category ──────────────────────────────
def assign_thresh(val, high, mid):
    """Assign a threshold label based on value cutoffs."""
    if val >= high:
        return THRESH_LABELS[0]
    elif val >= mid:
        return THRESH_LABELS[1]
    return THRESH_LABELS[2]


def thresh_color_scale():
    """Return a consistent Altair color scale for threshold categories."""
    return alt.Scale(domain=THRESH_LABELS, range=THRESH_COLORS)


# ── Load all datasets ────────────────────────────────────────────────
mode_recovery = load_mode_recovery()
metro_df = load_metro_recovery()
agency_df = load_agency_efficiency()
annual_totals = load_annual_totals()


# ── Header ───────────────────────────────────────────────────────────
st.title("US Public Transit Recovery & Efficiency")
st.markdown(
    "An analysis of US public transit ridership recovery relative to 2019 pre-COVID baselines, "
    "covering mode-level trends, metro-level variation, spending efficiency, and farebox cost "
    "coverage across 832 transit agencies reporting to the National Transit Database."
)

# ── KPI Metrics ──────────────────────────────────────────────────────
totals_2019 = annual_totals[annual_totals["report_year"] == 2019]["total_upt"].iloc[0]
totals_2024 = annual_totals[annual_totals["report_year"] == 2024]["total_upt"].iloc[0]
totals_2023 = annual_totals[annual_totals["report_year"] == 2023]["total_upt"].iloc[0]
recovery_overall = round(totals_2024 / totals_2019 * 100, 1)
yoy_change = round((totals_2024 - totals_2023) / totals_2023 * 100, 1)

col1, col2, col3, col4 = st.columns(4)
col1.metric("2024 Annual Ridership", f"{totals_2024 / 1e9:.1f}B trips",
            delta=f"{yoy_change:+.1f}% vs 2023")
col2.metric("2019 Baseline", f"{totals_2019 / 1e9:.1f}B trips")
col3.metric("National Recovery", f"{recovery_overall}%",
            delta=f"{recovery_overall - 100:.1f}pp vs 2019")
col4.metric("Active Agencies", f"{annual_totals[annual_totals['report_year'] == 2024]['agencies'].iloc[0]:,}")

st.divider()

# ═══════════════════════════════════════════════════════════════════════
# CHART 1: Annual ridership by transit mode
# ═══════════════════════════════════════════════════════════════════════
st.header("Annual national ridership by transit mode, indexed to 2019")
st.markdown(
    "Each line shows a mode's annual ridership as a percentage of its 2019 total. "
    "By 2024, bus reached 83% of its 2019 level, heavy rail (subway/metro) 75%, "
    "and commuter rail 73%. The 8-10 percentage point gap between bus and rail "
    "has persisted since 2021. Bus serves a higher share of transit-dependent riders "
    "who had no remote work option, while rail ridership is more concentrated among "
    "office commuters."
)

key_modes = ["Bus", "Heavy Rail", "Light Rail", "Commuter Rail"]
line_df = mode_recovery[mode_recovery["mode_name"].isin(key_modes)].copy()

selection = alt.selection_point(fields=["mode_name"], bind="legend")

recovery_line = (
    alt.Chart(line_df)
    .mark_line(strokeWidth=2.5, point=alt.OverlayMarkDef(size=40))
    .encode(
        x=alt.X("report_year:O", title="Year", axis=alt.Axis(labelAngle=0)),
        y=alt.Y("recovery_pct:Q", title="% of 2019 Ridership",
                 scale=alt.Scale(domain=[0, 130])),
        color=alt.Color(
            "mode_name:N",
            title="Transit Mode",
            scale=alt.Scale(
                domain=key_modes,
                range=[MODE_COLORS[m] for m in key_modes],
            ),
        ),
        strokeDash=alt.StrokeDash(
            "mode_name:N",
            title="Transit Mode",
            scale=alt.Scale(
                domain=key_modes,
                range=[[1, 0], [6, 4], [2, 2], [8, 2, 2, 2]],
            ),
        ),
        opacity=alt.condition(selection, alt.value(1), alt.value(0.15)),
        tooltip=[
            alt.Tooltip("mode_name:N", title="Mode"),
            alt.Tooltip("report_year:O", title="Year"),
            alt.Tooltip("recovery_pct:Q", title="% of 2019", format=".1f"),
            alt.Tooltip("total_upt:Q", title="Annual Trips (UPT)", format=",.0f"),
        ],
    )
    .properties(height=400)
    .add_params(selection)
)

ref_line = (
    alt.Chart(pd.DataFrame({"y": [100]}))
    .mark_rule(strokeDash=[4, 4], color=IBM_GREY, strokeWidth=1)
    .encode(y="y:Q")
)

ref_label = (
    alt.Chart(pd.DataFrame({"y": [100], "label": ["2019 Baseline"]}))
    .mark_text(align="left", dx=5, dy=-10, fontSize=13, color=IBM_GREY)
    .encode(y="y:Q", text="label:N")
)

st.altair_chart(recovery_line + ref_line + ref_label, use_container_width=True)

st.markdown(
    "<b>Source:</b> National Transit Database Monthly Module via data.transportation.gov Socrata API.<br>"
    "<b>Tools:</b> Bruin (pipeline), BigQuery (warehouse), Altair (visualization).<br>"
    "<b>Calculation:</b> (annual UPT / 2019 annual UPT) x 100, summed nationally per mode. "
    "Includes directly operated (DO) and purchased transportation (PT) services.<br>"
    "<b>Limitations:</b> UPT counts each boarding separately (a transfer = 2 UPTs). "
    "2025 includes Jan-Feb only and will appear lower than the full-year figure. "
    "Agencies that began reporting after 2019 have no baseline and are excluded.",
    unsafe_allow_html=True,
)

st.divider()

# ═══════════════════════════════════════════════════════════════════════
# CHART 2: WFH vs Recovery
# ═══════════════════════════════════════════════════════════════════════
st.header("Work-from-home rate vs. ridership recovery by metro area (2023)")
st.markdown(
    "Each dot is a metro area, positioned by its Census WFH rate (x-axis) and "
    "2023 ridership as a percentage of 2019 (y-axis), both from the same year. "
    "Dot size reflects population. "
    "The Pearson correlation is near zero, indicating no meaningful linear "
    "relationship between WFH rates and transit recovery. "
    "For example, Austin (high WFH) shows stronger recovery than San Juan (low WFH), "
    "suggesting service levels, fare policy, and rider demographics matter more "
    "than remote work rates."
)

wfh_df = metro_df[metro_df["wfh_rate_pct"].notna()].copy()

pop_filter = st.radio(
    "Filter by metro population",
    ["All", "Over 1 million", "Under 1 million"],
    horizontal=True,
)
if pop_filter == "Over 1 million":
    wfh_df = wfh_df[wfh_df["uza_population"] >= 1_000_000]
elif pop_filter == "Under 1 million":
    wfh_df = wfh_df[wfh_df["uza_population"] < 1_000_000]

wfh_corr = wfh_df[["wfh_rate_pct", "recovery_pct"]].corr().iloc[0, 1]

wfh_df["pop_label"] = (wfh_df["uza_population"] / 1e6).round(1).astype(str) + "M"
wfh_df["metro_short"] = (
    wfh_df["uza_name"]
    .str.replace(r"--.*", "", regex=True)
    .str.replace(r",.*", "", regex=True)
    .str.strip()
)

wfh_scatter = (
    alt.Chart(wfh_df)
    .mark_circle(opacity=0.75)
    .encode(
        x=alt.X("wfh_rate_pct:Q", title="Work-from-Home Rate (%)",
                 scale=alt.Scale(zero=False)),
        y=alt.Y("recovery_pct:Q", title="2023 Ridership Recovery (% of 2019)",
                 scale=alt.Scale(zero=False)),
        size=alt.Size("uza_population:Q", title="Metro Population",
                       scale=alt.Scale(range=[40, 700]),
                       legend=alt.Legend(
                           format=",.0f",
                           orient="top",
                           direction="horizontal",
                       )),
        color=alt.value(IBM_BLUE),
        tooltip=[
            alt.Tooltip("uza_name:N", title="Metro Area (UZA)"),
            alt.Tooltip("recovery_pct:Q", title="Recovery %", format=".1f"),
            alt.Tooltip("wfh_rate_pct:Q", title="WFH Rate %", format=".1f"),
            alt.Tooltip("uza_population:Q", title="Population", format=",.0f"),
            alt.Tooltip("total_upt:Q", title="2023 Trips (UPT)", format=",.0f"),
            alt.Tooltip("ridership_2019:Q", title="2019 Trips (UPT)", format=",.0f"),
        ],
    )
    .properties(height=700)
)

# Place labels below the dot for these cities to avoid overlap with neighbors
labels_below = {
    "Indianapolis", "Washington", "Philadelphia", "Seattle",
    "Denver", "San Juan", "Richmond", "Tucson",
    "Atlanta", "Minneapolis", "St. Louis",
    "Charlotte", "Pittsburgh", "Virginia Beach", "Columbus",
}
labels_left = {"Jacksonville"}

wfh_df["label_pos"] = wfh_df["metro_short"].apply(
    lambda x: "below" if x in labels_below else ("left" if x in labels_left else "above")
)

wfh_labels_above = (
    alt.Chart(wfh_df[wfh_df["label_pos"] == "above"])
    .mark_text(fontSize=11, dy=-18)
    .encode(
        x="wfh_rate_pct:Q",
        y="recovery_pct:Q",
        text="metro_short:N",
        color=alt.value(IBM_DARK),
    )
)

wfh_labels_below = (
    alt.Chart(wfh_df[wfh_df["label_pos"] == "below"])
    .mark_text(fontSize=11, dy=15)
    .encode(
        x="wfh_rate_pct:Q",
        y="recovery_pct:Q",
        text="metro_short:N",
        color=alt.value(IBM_DARK),
    )
)

wfh_labels_left = (
    alt.Chart(wfh_df[wfh_df["label_pos"] == "left"])
    .mark_text(fontSize=11, dx=-15, align="right")
    .encode(
        x="wfh_rate_pct:Q",
        y="recovery_pct:Q",
        text="metro_short:N",
        color=alt.value(IBM_DARK),
    )
)

st.altair_chart(wfh_scatter + wfh_labels_above + wfh_labels_below + wfh_labels_left, use_container_width=True)

st.markdown(
    f"<b>Source:</b> WFH rates from US Census Bureau ACS 1-Year Table B08301 (2023). "
    f"Recovery from NTD Monthly Module (2023 vs 2019 UPT). Both metrics from the same year.<br>"
    f"<b>Tools:</b> Bruin (pipeline), BigQuery (warehouse), Altair (visualization).<br>"
    f"<b>Calculation:</b> WFH rate = workers working from home / total workers. "
    f"Pearson r = {wfh_corr:.2f}, n = {len(wfh_df)} metros. "
    f"UZA-to-MSA matching on primary city name (~57% match rate).<br>"
    "<b>Limitations:</b> ACS 1-year estimates available only for areas with 65,000+ population. "
    "Margins of error are 1-3 percentage points for WFH rates. "
    "WFH rate reflects all workers, not just transit commuters. "
    "Metros without a Census ACS match are excluded.",
    unsafe_allow_html=True,
)

st.divider()

# ═══════════════════════════════════════════════════════════════════════
# CHART 3: Spending Efficiency
# ═══════════════════════════════════════════════════════════════════════
st.header("Transit operating expense vs. ridership per capita, by metro (2023)")
st.markdown(
    "Each dot is a metro area with >500K population, positioned by operating expense per "
    "capita (x-axis) and annual trips per capita (y-axis), both on log scale. "
    "There is a strong log-linear relationship (r ~ 0.9). "
    "New York delivers 184 trips per capita at $1,040/person in spending. "
    "Washington DC, at a comparable $693/person, produces 60 trips per capita. "
    "Metros below the trend line spend more per ride relative to the ridership they generate."
)

spend_df = metro_df[
    (metro_df["uza_population"] > 500000)
    & (metro_df["expense_per_capita"].notna())
    & (metro_df["trips_per_capita"].notna())
    & (metro_df["expense_per_capita"] > 0)
    & (metro_df["trips_per_capita"] > 0)
].copy()

spend_df["metro_short"] = (
    spend_df["uza_name"]
    .str.replace(r"--.*", "", regex=True)
    .str.replace(r",.*", "", regex=True)
    .str.strip()
)

spend_label_names = [
    "New York", "San Francisco", "Chicago", "Washington", "Los Angeles",
    "Boston", "Seattle", "Honolulu", "Philadelphia", "Miami",
    "Houston", "Dallas", "Atlanta", "Denver", "Las Vegas", "San Diego",
    "Portland", "Detroit", "Salt Lake City", "Pittsburgh",
]
spend_df["show_label"] = spend_df["metro_short"].isin(spend_label_names)

spend_scatter = (
    alt.Chart(spend_df)
    .mark_circle(opacity=0.75, size=90)
    .encode(
        x=alt.X("expense_per_capita:Q",
                 title="Transit Operating Expense per Capita ($, log scale)",
                 scale=alt.Scale(type="log")),
        y=alt.Y("trips_per_capita:Q",
                 title="Annual Trips per Capita (log scale)",
                 scale=alt.Scale(type="log")),
        color=alt.value(IBM_ORANGE),
        tooltip=[
            alt.Tooltip("uza_name:N", title="Metro Area (UZA)"),
            alt.Tooltip("expense_per_capita:Q", title="Expense per Capita", format="$,.0f"),
            alt.Tooltip("trips_per_capita:Q", title="Trips per Capita", format=",.1f"),
            alt.Tooltip("metro_cost_per_trip:Q", title="Cost per Trip", format="$,.2f"),
            alt.Tooltip("metro_fare_recovery:Q", title="Fare Cost Coverage", format=".1%"),
            alt.Tooltip("uza_population:Q", title="Population", format=",.0f"),
            alt.Tooltip("total_upt:Q", title="Total 2023 Trips", format=",.0f"),
        ],
    )
    .properties(height=480)
)

spend_labels_chart = (
    alt.Chart(spend_df[spend_df["show_label"]])
    .mark_text(fontSize=12, dy=-12, fontWeight="bold")
    .encode(
        x=alt.X("expense_per_capita:Q", scale=alt.Scale(type="log")),
        y=alt.Y("trips_per_capita:Q", scale=alt.Scale(type="log")),
        text="metro_short:N",
        color=alt.value(IBM_DARK),
    )
)

st.altair_chart(spend_scatter + spend_labels_chart, use_container_width=True)

log_corr = np.corrcoef(
    np.log(spend_df["expense_per_capita"]),
    np.log(spend_df["trips_per_capita"]),
)[0, 1]

st.markdown(
    f"<b>Source:</b> NTD Annual Metrics (2023) for operating expenses and UPT. "
    f"UZA population from NTD records.<br>"
    "<b>Tools:</b> Bruin (pipeline), BigQuery (warehouse), Altair (visualization).<br>"
    f"<b>Calculation:</b> Trips per capita = total UPT / UZA population. "
    f"Expense per capita = total operating expenses / UZA population. "
    f"Both axes on log scale. Log-log Pearson r = {log_corr:.2f}, n = {len(spend_df)} metros with >500K population.<br>"
    "<b>Limitations:</b> UZA population may not reflect the full transit service area; "
    "some agencies serve beyond UZA boundaries. "
    "Operating expenses include all modes and may differ structurally between "
    "rail-heavy and bus-only systems.",
    unsafe_allow_html=True,
)

st.divider()

# ═══════════════════════════════════════════════════════════════════════
# CHART 4: Farebox Cost Coverage
# ═══════════════════════════════════════════════════════════════════════
st.header("Fare revenue as a share of operating costs, by agency (2024)")
st.markdown(
    "Each bar shows the percentage of an agency's operating costs covered by fare revenue "
    "(this metric is unrelated to COVID ridership recovery). "
    "The 30 largest US transit agencies by 2024 annual ridership (>1M trips) are shown. "
    "Bars are colored by coverage tier: green (>25%), blue (15-25%), red (<15%). "
    "Three agencies exceed 25%: MTA Metro-North, MTA NYC Transit, and PATH. "
    "Most bus-dominant agencies cover under 10% of costs from fares; "
    "the remainder is funded by federal, state, and local subsidies."
)

farebox_df = agency_df.nlargest(30, "total_upt").copy()
farebox_df["city"] = (
    farebox_df["uza_name"]
    .str.replace(r"--.*", "", regex=True)
    .str.replace(r",.*", "", regex=True)
    .str.strip()
)
farebox_df["agency_short"] = (
    farebox_df["agency"].str.replace(r",?\s*dba:.*", "", regex=True).str[:40]
    + " (" + farebox_df["city"] + ")"
)
farebox_df["fare_pct"] = farebox_df["fare_recovery_ratio"] * 100

FARE_LABELS = ["Above 25%", "15-25%", "Below 15%"]
FARE_COLORS = [IBM_GREEN, IBM_BLUE, IBM_RED]

farebox_df["fare_status"] = farebox_df["fare_pct"].apply(
    lambda x: FARE_LABELS[0] if x >= 25 else (FARE_LABELS[1] if x >= 15 else FARE_LABELS[2])
)

farebox_bar = (
    alt.Chart(farebox_df)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    .encode(
        x=alt.X("fare_pct:Q", title="Fare Revenue / Operating Costs (%)",
                 scale=alt.Scale(domain=[0, 40])),
        y=alt.Y("agency_short:N", title="", sort="-x",
                 axis=alt.Axis(labelLimit=350)),
        color=alt.Color(
            "fare_status:N",
            title="Cost Coverage",
            scale=alt.Scale(domain=FARE_LABELS, range=FARE_COLORS),
        ),
        tooltip=[
            alt.Tooltip("agency:N", title="Agency"),
            alt.Tooltip("state:N", title="State"),
            alt.Tooltip("fare_pct:Q", title="Fare Cost Coverage %", format=".1f"),
            alt.Tooltip("total_upt:Q", title="Annual Trips (UPT)", format=",.0f"),
            alt.Tooltip("total_operating_expenses:Q", title="Operating Expenses", format="$,.0f"),
            alt.Tooltip("total_fare_revenue:Q", title="Fare Revenue", format="$,.0f"),
            alt.Tooltip("cost_per_trip:Q", title="Cost per Trip", format="$,.2f"),
            alt.Tooltip("primary_mode:N", title="Primary Mode"),
        ],
    )
    .properties(height=750)
)

farebox_labels = (
    alt.Chart(farebox_df)
    .mark_text(align="left", dx=5, fontSize=13, fontWeight="bold")
    .encode(
        x="fare_pct:Q",
        y=alt.Y("agency_short:N", sort="-x"),
        text=alt.Text("fare_pct:Q", format=".1f"),
        color=alt.Color(
            "fare_status:N",
            scale=alt.Scale(domain=FARE_LABELS, range=FARE_COLORS),
            legend=None,
        ),
    )
)

st.altair_chart(farebox_bar + farebox_labels, use_container_width=True)

total_fare = farebox_df["total_fare_revenue"].sum()
total_opex = farebox_df["total_operating_expenses"].sum()
agg_farebox = total_fare / total_opex * 100

st.markdown(
    "<b>Source:</b> NTD Annual Metrics (2024), Federal Transit Administration, "
    "via data.transportation.gov Socrata API.<br>"
    "<b>Tools:</b> Bruin (pipeline), BigQuery (warehouse), Altair (visualization).<br>"
    f"<b>Calculation:</b> Farebox ratio = total fare revenue / total operating expenses. "
    f"Top 30 agencies by annual UPT shown. Aggregate ratio across these agencies: {agg_farebox:.1f}%.<br>"
    "<b>Limitations:</b> Fare revenue includes passes and pre-paid fares but excludes advertising, "
    "contract revenue, and subsidies. Operating expenses cover the full cost of service delivery. "
    "NTD annual financial data is limited to 2022-2024 reporting years. "
    "Agencies showing 0% have adopted fare-free policies.",
    unsafe_allow_html=True,
)

st.divider()

# ── Methodology ──────────────────────────────────────────────────────
st.header("Methodology")
st.markdown(
    "Source: **National Transit Database** (FTA/DOT), monthly module and annual metrics via "
    "**data.transportation.gov** Socrata API; **US Census Bureau** ACS 1-Year Table B08301."
    "<br>"
    "Tools: **Bruin** (pipeline orchestration), **BigQuery** (data warehouse), **Altair** (charts)."
    "<br>"
    "Color palette: **IBM Design for Accessibility** - colorblind-safe categorical palette.",
    unsafe_allow_html=True,
)

with st.expander("Data pipeline details"):
    st.markdown("""
    **Pipeline**: `public-transit-analysis/` (6 Bruin assets)

    **Raw data sources**:
    - NTD Monthly Module: 365,069 rows, 834 agencies, Jan 2002 - Feb 2026 (Socrata API `8bui-9xvu`)
    - NTD Annual Metrics: 11,112 rows, 2,299 agencies, 2022-2024 (Socrata API `ekg5-frzt`)
    - Census ACS B08301: 981 rows, 323 MSAs, 2019-2023 (Census API, no key required)

    **Transformations**:
    - Deduplicated raw data by latest extraction timestamp
    - Aggregated DO + PT service types per agency/mode
    - Computed 2019 same-month baselines for recovery %
    - Joined NTD UZA to Census MSA using primary city name matching (~57% match rate)
    - Carried forward latest Census year for years without ACS data

    **Quality checks**: 20 automated checks (not_null, non_negative) on staging tables. All passing.
    """)

with st.expander("Key definitions"):
    st.markdown("""
    - **UPT (Unlinked Passenger Trips)**: Each boarding = 1 trip. A ride with a transfer = 2 UPTs.
    - **VRM (Vehicle Revenue Miles)**: Miles traveled in revenue service.
    - **Recovery %**: (Current period UPT / Same period 2019 UPT) x 100.
    - **Farebox Cost Coverage**: Fare revenue / operating expenses. Measures financial self-sufficiency.
    - **Cost per Trip**: Operating expenses / UPT.
    - **UZA**: Urbanized Area (Census geographic unit, used by NTD for metro-level reporting).
    - **MSA**: Metropolitan Statistical Area (Census economic unit, used for ACS commuting data).
    """)
