from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(page_title="OECD Pension Systems: A Cross-Country Snapshot", layout="wide")

PROJECT_ID = "bruin-playground-arsalan"
base_path = Path(__file__).parent

# Wong (2011) colorblind-safe palette — distinguishable for deuteranopia, protanopia, tritanopia.
VERMILLION = "#D55E00"
SKY_BLUE = "#56B4E9"
ORANGE = "#E69F00"
BLUE_GREEN = "#009E73"
PURPLE = "#CC79A7"
BLUE = "#0072B2"
YELLOW = "#F0E442"
GREY = "#999999"

FOCUS_COUNTRIES = ["JPN", "ITA", "DEU", "USA", "KOR", "MEX", "TUR"]
FOCUS_COLORS = [VERMILLION, ORANGE, BLUE_GREEN, BLUE, PURPLE, SKY_BLUE, GREY]

# Reference years pinned in staging.pc_pension_system (apples-to-apples).
REF_YEAR_RETIREMENT = 2024
REF_YEAR_REPLACEMENT = 2024
REF_YEAR_SPENDING = 2021
REF_YEAR_ASSETS = 2023


@st.cache_resource
def get_client():
    credentials = service_account.Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)


@st.cache_data(ttl=86400)
def run_query(filename: str) -> pd.DataFrame:
    sql = (base_path / filename).read_text()
    return get_client().query(sql).to_dataframe()


# ── Load data ────────────────────────────────────────────────────────────────

dep_traj = run_query("dependency_trajectory.sql")
retirement_gap = run_query("retirement_gap.sql")
spending = run_query("spending_vs_dependency.sql")
mercer = run_query("mercer_sustainability.sql")
fiscal = run_query("fiscal_pressure.sql")
score_outcome = run_query("score_vs_outcome.sql")

latest_observed_year = int(dep_traj.loc[~dep_traj["is_projection"], "year"].max())
projection_year = 2050

# ── Header ───────────────────────────────────────────────────────────────────

st.title("OECD Pension Systems: A Cross-Country Snapshot")
st.caption(
    "Scope: 38 OECD member countries. "
    "Every indicator below is pinned to a single reference year and uses one harmonized methodology, "
    "so every cross-country comparison is apples-to-apples. Reference years: "
    f"retirement age = {REF_YEAR_RETIREMENT}, net replacement rate = {REF_YEAR_REPLACEMENT}, "
    f"public pension spending = {REF_YEAR_SPENDING}, pension assets = {REF_YEAR_ASSETS}, "
    f"demographic indicators = {latest_observed_year} (observed) and {projection_year} (UN WPP 2024 medium variant)."
)

# Key figures
n_countries = dep_traj["iso3_code"].nunique()
oldest_now = (
    dep_traj[dep_traj["year"] == latest_observed_year]
    .sort_values("old_age_dep_ratio", ascending=False)
    .head(1)
)
oldest_2050 = (
    dep_traj[dep_traj["year"] == projection_year]
    .sort_values("old_age_dep_ratio", ascending=False)
    .head(1)
)
youngest_2050 = (
    dep_traj[dep_traj["year"] == projection_year]
    .sort_values("old_age_dep_ratio", ascending=True)
    .head(1)
)

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("OECD countries covered", f"{n_countries}")
with col2:
    if not oldest_now.empty:
        st.metric(
            f"Highest dependency ratio ({latest_observed_year})",
            oldest_now.iloc[0]["country_name"],
            f"{oldest_now.iloc[0]['old_age_dep_ratio']:.1f} per 100 workers",
            delta_color="off",
        )
with col3:
    if not oldest_2050.empty:
        st.metric(
            f"Highest projected ({projection_year})",
            oldest_2050.iloc[0]["country_name"],
            f"{oldest_2050.iloc[0]['old_age_dep_ratio']:.1f} per 100 workers",
            delta_color="off",
        )
with col4:
    if not youngest_2050.empty:
        st.metric(
            f"Lowest projected ({projection_year})",
            youngest_2050.iloc[0]["country_name"],
            f"{youngest_2050.iloc[0]['old_age_dep_ratio']:.1f} per 100 workers",
            delta_color="off",
        )

st.divider()

# ── Chart 1: Old-age dependency ratio trajectory ─────────────────────────────

st.subheader("Chart 1 — Old-age dependency ratio for seven OECD countries, 1990 to 2050")
st.markdown(
    "**Insight.** Every country in the set has seen its old-age dependency ratio rise since 1990, "
    f"and the projected trajectory to {projection_year} shows continued increase in all seven. "
    "Japan and Korea sit at the steeper end of the projected curve; Mexico and Türkiye remain lower "
    "in absolute terms but show the same upward direction. The ratio is the number of people aged "
    "65+ per 100 working-age adults (15–64) — a direct proxy for the number of retirees each worker "
    "must support through taxes and contributions."
)

focus_df = dep_traj[dep_traj["iso3_code"].isin(FOCUS_COUNTRIES)].copy()
focus_df["series"] = focus_df["is_projection"].map({False: "Historical", True: "Projection"})

dep_line = (
    alt.Chart(focus_df)
    .mark_line(strokeWidth=2.5)
    .encode(
        x=alt.X("year:Q", title="Year", scale=alt.Scale(domain=[1990, 2050]), axis=alt.Axis(format="d")),
        y=alt.Y(
            "old_age_dep_ratio:Q",
            title="Old-age dependency ratio (people aged 65+ per 100 people aged 15–64)",
            scale=alt.Scale(zero=True),
        ),
        color=alt.Color(
            "country_name:N",
            title="Country",
            scale=alt.Scale(
                domain=focus_df["country_name"].unique().tolist(),
                range=FOCUS_COLORS[: focus_df["country_name"].nunique()],
            ),
        ),
        strokeDash=alt.StrokeDash(
            "series:N",
            title="Series",
            scale=alt.Scale(
                domain=["Historical", "Projection"],
                range=[[1, 0], [4, 4]],
            ),
        ),
        tooltip=[
            alt.Tooltip("country_name:N", title="Country"),
            alt.Tooltip("year:Q", title="Year", format="d"),
            alt.Tooltip("old_age_dep_ratio:Q", title="Dependency ratio", format=",.1f"),
            alt.Tooltip("series:N", title="Series"),
        ],
    )
    .properties(height=380)
)

projection_rule = (
    alt.Chart(pd.DataFrame({"year": [latest_observed_year]}))
    .mark_rule(color=GREY, strokeDash=[2, 2])
    .encode(x="year:Q")
)
projection_label = (
    alt.Chart(
        pd.DataFrame({"year": [latest_observed_year], "label": [f"Projections from {latest_observed_year + 1} →"]})
    )
    .mark_text(color=GREY, dx=6, dy=-160, align="left", fontSize=11)
    .encode(x="year:Q", text="label:N")
)

st.altair_chart(dep_line + projection_rule + projection_label, use_container_width=True)

median_1990 = dep_traj[dep_traj["year"] == 1990]["old_age_dep_ratio"].median()
median_now = dep_traj[dep_traj["year"] == latest_observed_year]["old_age_dep_ratio"].median()
median_2050 = dep_traj[dep_traj["year"] == 2050]["old_age_dep_ratio"].median()
st.markdown(
    f"> Median OECD-38 old-age dependency ratio: **{median_1990:.1f}** in 1990 → "
    f"**{median_now:.1f}** in {latest_observed_year} → **{median_2050:.1f}** projected in 2050."
)
st.caption(
    "**Source.** United Nations, World Population Prospects 2024 (released 11 July 2024), medium-variant projections.  \n"
    "**Methodology.** Old-age dependency ratio = population aged 65+ divided by population aged 15–64, × 100. "
    "Historical estimates through 2024; medium-variant projections 2025–2050. UN-DESA applies a single methodology to all locations.  \n"
    f"**Country set.** {', '.join(FOCUS_COUNTRIES)} — selected to span the observed OECD range (from Japan and Italy at the high end to Mexico and Türkiye at the low end).  \n"
    "**Tools.** Data ingested via SDMX CSV download, stored in BigQuery, rendered with Altair.  \n"
    "**Limitations.** Medium-variant is a single scenario; the low- and high-variant projections diverge noticeably by 2050. "
    "Solid lines = historical; dashed = projected."
)

st.divider()

# ── Chart 2: Retirement age vs life expectancy at 65 ─────────────────────────

st.subheader(
    f"Chart 2 — Statutory retirement age ({REF_YEAR_RETIREMENT}) vs. remaining life expectancy at 65 ({latest_observed_year})"
)
st.markdown(
    "**Insight.** The implied number of years a full-career worker spends in retirement — the vertical "
    "distance above the 45° line where retirement age equals 65 plus life expectancy — varies widely across "
    "the OECD. Countries with lower statutory retirement ages and high life expectancy (upper-left of the chart) "
    "imply the longest average retirements; countries with high retirement ages and moderate life expectancy "
    "(lower-right) imply the shortest. Dashed reference lines mark constant years-in-retirement at 15, 20, and 25."
)

if retirement_gap.empty:
    st.warning("No rows to display — OECD retirement-age data has not yet been loaded.")
else:
    rl_data = []
    for years in (15, 20, 25):
        for ret_age in range(50, 70):
            rl_data.append(
                {"retirement_age_latest": ret_age, "le65": (ret_age - 65) + years, "years": f"{years} yrs in retirement"}
            )
    rl_df = pd.DataFrame(rl_data)

    ref_lines = (
        alt.Chart(rl_df)
        .mark_line(color=GREY, strokeDash=[3, 3], strokeWidth=1)
        .encode(
            x=alt.X("retirement_age_latest:Q"),
            y=alt.Y("le65:Q"),
            detail="years:N",
        )
    )
    ref_labels = (
        alt.Chart(rl_df[rl_df["retirement_age_latest"] == 51].copy())
        .mark_text(color=GREY, align="left", dx=4, dy=-6, fontSize=11)
        .encode(x="retirement_age_latest:Q", y="le65:Q", text="years:N")
    )

    points = (
        alt.Chart(retirement_gap)
        .mark_circle(size=140, opacity=0.85)
        .encode(
            x=alt.X(
                "retirement_age_latest:Q",
                title="Statutory retirement age, years (full career, entry at age 22)",
                scale=alt.Scale(zero=False, nice=True),
            ),
            y=alt.Y(
                "life_expectancy_at_65_today:Q",
                title="Remaining life expectancy at age 65, years",
                scale=alt.Scale(zero=False, nice=True),
            ),
            color=alt.Color(
                "region:N",
                title="Region",
                scale=alt.Scale(range=[SKY_BLUE, ORANGE, BLUE_GREEN, VERMILLION, PURPLE]),
            ),
            tooltip=[
                alt.Tooltip("country_name:N", title="Country"),
                alt.Tooltip("retirement_age_latest:Q", title="Retirement age", format=".1f"),
                alt.Tooltip("life_expectancy_at_65_today:Q", title="Life exp at 65", format=".1f"),
                alt.Tooltip("years_in_retirement_today:Q", title="Years retired", format=".1f"),
                alt.Tooltip("region:N", title="Region"),
            ],
        )
    )
    labels = (
        alt.Chart(retirement_gap)
        .mark_text(align="left", dx=7, dy=-4, fontSize=10)
        .encode(x="retirement_age_latest:Q", y="life_expectancy_at_65_today:Q", text="iso3_code:N")
    )

    st.altair_chart((ref_lines + ref_labels + points + labels).properties(height=480), use_container_width=True)

    med_years = retirement_gap["years_in_retirement_today"].median()
    longest = retirement_gap.sort_values("years_in_retirement_today", ascending=False).head(1)
    shortest = retirement_gap.sort_values("years_in_retirement_today", ascending=True).head(1)
    st.markdown(
        f"> Median implied years-in-retirement across the OECD-38: **{med_years:.1f}**. "
        f"Highest: **{longest.iloc[0]['country_name']}** ({longest.iloc[0]['years_in_retirement_today']:.1f}). "
        f"Lowest: **{shortest.iloc[0]['country_name']}** ({shortest.iloc[0]['years_in_retirement_today']:.1f})."
    )
    st.caption(
        f"**Sources.** OECD Pensions at a Glance (dataflow `DSD_PAG@DF_DPS`, measure `CRPLF22` — "
        f"Current Retirement, Pension Length-of-service, Full-career, entry age 22), reference year {REF_YEAR_RETIREMENT}. "
        f"Life expectancy at 65 from United Nations WPP 2024, abridged life tables, both sexes, reference year {latest_observed_year}.  \n"
        "**Methodology.** Statutory retirement age is the average of the male and female values where both are reported. "
        "Years-in-retirement derived as (life expectancy at 65) − (retirement age − 65).  \n"
        "**Tools.** OECD SDMX CSV download, UN WPP CSV download, BigQuery joins, Altair scatter plot.  \n"
        "**Limitations.** Statutory retirement age is the legal norm; actual effective retirement age differs in many "
        "OECD countries due to early-retirement pathways. Life-expectancy figures are period (not cohort) estimates."
    )

st.divider()

# ── Chart 3: Pension spending %GDP vs old-age dependency ─────────────────────

st.subheader(
    f"Chart 3 — Public pension spending (% of GDP, {REF_YEAR_SPENDING}) against old-age dependency ratio, today vs. 2050"
)
st.markdown(
    "**Insight.** Each OECD country appears twice on the vertical axis — once at its latest observed "
    f"dependency ratio ({latest_observed_year}, circles) and once at the projected {projection_year} "
    "value (triangles). The horizontal axis is unchanged (pension spending is at a single reference "
    f"year of {REF_YEAR_SPENDING}). The vertical distance between a country's two markers shows how "
    "much additional demographic pressure is still ahead, independent of current spending levels."
)

spending_long = spending.dropna(subset=["pension_spending_pct_gdp_latest"]).copy()
if spending_long.empty:
    st.warning("No spending data yet — run `raw.pc_oecd_pension_spending` first.")
else:
    plot_rows = []
    for _, row in spending_long.iterrows():
        if pd.notna(row["old_age_dep_ratio_today"]):
            plot_rows.append(
                {
                    **row.to_dict(),
                    "period": f"{latest_observed_year} (observed)",
                    "dep_ratio": row["old_age_dep_ratio_today"],
                }
            )
        if pd.notna(row["old_age_dep_ratio_2050"]):
            plot_rows.append(
                {
                    **row.to_dict(),
                    "period": f"{projection_year} (projected)",
                    "dep_ratio": row["old_age_dep_ratio_2050"],
                }
            )
    sp_plot = pd.DataFrame(plot_rows)

    scatter = (
        alt.Chart(sp_plot)
        .mark_point(size=130, opacity=0.85, filled=True)
        .encode(
            x=alt.X(
                "pension_spending_pct_gdp_latest:Q",
                title=f"Public pension spending, % of GDP ({REF_YEAR_SPENDING})",
                scale=alt.Scale(zero=True),
            ),
            y=alt.Y(
                "dep_ratio:Q",
                title="Old-age dependency ratio (per 100 working-age)",
                scale=alt.Scale(zero=True),
            ),
            color=alt.Color(
                "period:N",
                title="Observation period",
                scale=alt.Scale(
                    domain=[f"{latest_observed_year} (observed)", f"{projection_year} (projected)"],
                    range=[SKY_BLUE, VERMILLION],
                ),
            ),
            shape=alt.Shape(
                "period:N",
                title="Observation period",
                scale=alt.Scale(
                    domain=[f"{latest_observed_year} (observed)", f"{projection_year} (projected)"],
                    range=["circle", "triangle-up"],
                ),
            ),
            tooltip=[
                alt.Tooltip("country_name:N", title="Country"),
                alt.Tooltip("period:N", title="Period"),
                alt.Tooltip("pension_spending_pct_gdp_latest:Q", title="Spending % GDP", format=".1f"),
                alt.Tooltip("dep_ratio:Q", title="Dep. ratio", format=".1f"),
            ],
        )
        .properties(height=420)
        .resolve_scale(color="shared", shape="shared")
    )

    st.altair_chart(scatter, use_container_width=True)

    pair_df = spending_long.dropna(subset=["old_age_dep_ratio_today", "old_age_dep_ratio_2050"])
    median_increase = (pair_df["old_age_dep_ratio_2050"] - pair_df["old_age_dep_ratio_today"]).median()
    st.markdown(
        f"> Median OECD old-age dependency ratio is projected to rise by **+{median_increase:.1f} per 100 workers** "
        f"between {latest_observed_year} and {projection_year}."
    )
    st.caption(
        f"**Sources.** Public pension spending from OECD Pensions at a Glance (dataflow `DSD_PAG@DF_PAG`, "
        f"measure `PEP` — Public Expenditure on Pensions as % of GDP), reference year {REF_YEAR_SPENDING} "
        "(chosen as the latest year with full OECD-38 reporting coverage). Dependency ratios from UN WPP 2024.  \n"
        f"**Methodology.** Each country plotted twice on the y-axis (observed {latest_observed_year} vs. "
        f"projected {projection_year}); x-position is fixed because spending is a single reference year. "
        "Markers distinguished by shape (circle/triangle) and the Wong colorblind-safe palette.  \n"
        "**Tools.** OECD SDMX CSV, UN WPP CSV, BigQuery, Altair point plot.  \n"
        "**Limitations.** Public pension spending excludes private / occupational pension payouts, which are "
        "material in funded systems (Netherlands, Denmark). Spending at a single reference year does not "
        "capture rising-cost trajectories already under way."
    )

st.divider()

# ── Chart 4: Mercer sustainability vs 2050 dependency ────────────────────────

st.subheader(
    f"Chart 4 — Mercer sustainability sub-index (2025) vs. projected {projection_year} old-age dependency"
)
st.markdown(
    "**Insight.** The Mercer CFA Institute Global Pension Index rates pension systems on coverage, asset "
    "accumulation, demographics, and contribution levels using a single published methodology. Plotting "
    "the sustainability sub-index against projected 2050 dependency makes it possible to identify which "
    "OECD systems face the largest gap between their current sustainability posture and the demographic "
    "pressure they will face. Marker size encodes pension fund assets as a share of GDP — a measure of "
    "how much pre-funding each system has in place."
)

mercer_plot = mercer.dropna(subset=["mercer_sustainability", "old_age_dep_ratio_2050"]).copy()
if mercer_plot.empty:
    st.warning(
        "Mercer 2025 data is empty. Populate `assets/raw/seeds/mercer_gpi_2025.csv` and re-run "
        "`raw.pc_mercer_index_2025`."
    )
else:
    mercer_plot["size_val"] = mercer_plot["pension_assets_pct_gdp_latest"].fillna(5.0)
    m_scatter = (
        alt.Chart(mercer_plot)
        .mark_circle(opacity=0.85)
        .encode(
            x=alt.X(
                "mercer_sustainability:Q",
                title="Mercer sustainability sub-index, 0–100 (higher = more sustainable)",
                scale=alt.Scale(zero=False, nice=True),
            ),
            y=alt.Y(
                "old_age_dep_ratio_2050:Q",
                title=f"Projected old-age dependency ratio, {projection_year}",
                scale=alt.Scale(zero=True),
            ),
            size=alt.Size(
                "size_val:Q",
                title=[f"Pension assets", f"(% GDP, {REF_YEAR_ASSETS})"],
                scale=alt.Scale(range=[80, 900]),
            ),
            color=alt.Color(
                "region:N",
                title="Region",
                scale=alt.Scale(range=[SKY_BLUE, ORANGE, BLUE_GREEN, VERMILLION, PURPLE]),
            ),
            tooltip=[
                alt.Tooltip("country_name:N", title="Country"),
                alt.Tooltip("mercer_sustainability:Q", title="Sustainability", format=".1f"),
                alt.Tooltip("mercer_overall:Q", title="Mercer overall", format=".1f"),
                alt.Tooltip("old_age_dep_ratio_2050:Q", title="2050 dep ratio", format=".1f"),
                alt.Tooltip("pension_assets_pct_gdp_latest:Q", title="Assets % GDP", format=".1f"),
            ],
        )
        .properties(height=480)
    )
    m_labels = (
        alt.Chart(mercer_plot)
        .mark_text(align="left", dx=8, dy=-4, fontSize=10)
        .encode(x="mercer_sustainability:Q", y="old_age_dep_ratio_2050:Q", text="iso3_code:N")
    )
    st.altair_chart(m_scatter + m_labels, use_container_width=True)

    corr = mercer_plot[["mercer_sustainability", "old_age_dep_ratio_2050"]].corr().iloc[0, 1]
    st.markdown(
        f"> Pearson correlation between Mercer sustainability score and projected {projection_year} "
        f"dependency ratio across the {len(mercer_plot)} OECD countries scored by Mercer: **r = {corr:+.2f}**."
    )
    st.caption(
        "**Sources.** Mercer CFA Institute Global Pension Index 2025 (released October 2025), sustainability "
        "sub-index (0–100 scale). Dependency ratios from UN WPP 2024 medium variant. Pension fund assets "
        f"from OECD Global Pension Statistics (dataflow `DSD_FP@DF_PA`, unit `PT_B1GQ`, "
        f"vehicle type `_T` — all financing vehicles combined), reference year {REF_YEAR_ASSETS}.  \n"
        "**Methodology.** Mercer sustainability scores weight 13 sub-indicators spanning pension coverage, "
        "asset levels, demographics, and contribution rates. Pension assets measured as % of GDP using the "
        "total financing-vehicle dimension (pension funds + insurance contracts + banks + investment companies + other). "
        "Marker size scaled to assets % of GDP; non-scored OECD countries are excluded from this chart.  \n"
        "**Tools.** Mercer 2025 report (PDF) transcribed into a versioned seed CSV; OECD SDMX and UN WPP "
        "ingested via Python; BigQuery joins; Altair bubble scatter.  \n"
        "**Limitations.** Mercer sub-index weightings are opinionated (published methodology but not reproduced "
        "by a competing index). Mercer covers 52 countries globally; only the OECD intersection (≈28 of 38) "
        "appears here. Correlation is a point-in-time snapshot and does not imply causation."
    )

st.divider()

# ── Chart 5: Public debt × 2050 dependency ───────────────────────────────────

latest_debt_year = (
    int(fiscal["public_debt_pct_gdp_year"].max()) if not fiscal.empty else None
)

st.subheader(
    f"Chart 5 — General-government gross debt "
    f"({'/'.join(sorted({str(int(y)) for y in fiscal['public_debt_pct_gdp_year'].dropna().unique()}))}) "
    f"against projected {projection_year} old-age dependency"
)
st.markdown(
    "**Insight.** This chart overlays two independent sources of fiscal pressure: the stock of public "
    "debt a government already carries (IMF World Economic Outlook) and the demographic pressure it "
    f"will face by {projection_year} (UN WPP 2024). Countries in the **upper-right quadrant** enter the "
    "demographic transition with both elevated debt and steep projected dependency growth. Marker size "
    f"encodes current public pension spending (% of GDP, {REF_YEAR_SPENDING}) — larger bubbles indicate "
    "that a larger share of the fiscal burden is already committed to retirees."
)

if fiscal.empty:
    st.warning("No fiscal data yet — run `raw.pc_imf_public_debt` and rerun the profile.")
else:
    f_plot = fiscal.copy()
    f_plot["size_val"] = f_plot["pension_spending_pct_gdp_latest"].fillna(3.0)

    median_debt = f_plot["public_debt_pct_gdp_latest"].median()
    median_dep = f_plot["old_age_dep_ratio_2050"].median()

    quadrants = (
        alt.Chart(
            pd.DataFrame(
                {
                    "x": [median_debt, None],
                    "y": [None, median_dep],
                }
            )
        )
        .transform_fold(["x", "y"], as_=["axis", "val"])
    )
    rule_vert = (
        alt.Chart(pd.DataFrame({"x": [median_debt]}))
        .mark_rule(color=GREY, strokeDash=[3, 3], strokeWidth=1)
        .encode(x="x:Q")
    )
    rule_horiz = (
        alt.Chart(pd.DataFrame({"y": [median_dep]}))
        .mark_rule(color=GREY, strokeDash=[3, 3], strokeWidth=1)
        .encode(y="y:Q")
    )

    fiscal_scatter = (
        alt.Chart(f_plot)
        .mark_circle(opacity=0.85)
        .encode(
            x=alt.X(
                "public_debt_pct_gdp_latest:Q",
                title="General-government gross debt, % of GDP (IMF WEO)",
                scale=alt.Scale(zero=True, nice=True),
            ),
            y=alt.Y(
                "old_age_dep_ratio_2050:Q",
                title=f"Projected old-age dependency ratio, {projection_year}",
                scale=alt.Scale(zero=True, nice=True),
            ),
            size=alt.Size(
                "size_val:Q",
                title=[
                    "Pension spending",
                    f"(% GDP, {REF_YEAR_SPENDING})",
                ],
                scale=alt.Scale(range=[80, 900]),
            ),
            color=alt.Color(
                "region:N",
                title="Region",
                scale=alt.Scale(range=[SKY_BLUE, ORANGE, BLUE_GREEN, VERMILLION, PURPLE]),
            ),
            tooltip=[
                alt.Tooltip("country_name:N", title="Country"),
                alt.Tooltip("public_debt_pct_gdp_latest:Q", title="Debt % GDP", format=".1f"),
                alt.Tooltip("public_debt_pct_gdp_year:Q", title="Debt year", format="d"),
                alt.Tooltip("public_debt_pct_gdp_2030:Q", title="Debt % GDP (2030 WEO)", format=".1f"),
                alt.Tooltip("old_age_dep_ratio_2050:Q", title=f"{projection_year} dep ratio", format=".1f"),
                alt.Tooltip("pension_spending_pct_gdp_latest:Q", title="Pension spending % GDP", format=".1f"),
            ],
        )
        .properties(height=500)
    )
    fiscal_labels = (
        alt.Chart(f_plot)
        .mark_text(align="left", dx=10, dy=-4, fontSize=10)
        .encode(
            x="public_debt_pct_gdp_latest:Q",
            y="old_age_dep_ratio_2050:Q",
            text="iso3_code:N",
        )
    )

    st.altair_chart(
        rule_vert + rule_horiz + fiscal_scatter + fiscal_labels,
        use_container_width=True,
    )

    upper_right = f_plot[
        (f_plot["public_debt_pct_gdp_latest"] > median_debt)
        & (f_plot["old_age_dep_ratio_2050"] > median_dep)
    ].sort_values("public_debt_pct_gdp_latest", ascending=False)
    upper_right_names = ", ".join(upper_right["country_name"].head(8).tolist())
    st.markdown(
        f"> Upper-right quadrant (both debt and {projection_year} dependency above the OECD median of "
        f"{median_debt:.0f}% of GDP and {median_dep:.1f} per 100 workers): "
        f"**{len(upper_right)} of {len(f_plot)} countries** — {upper_right_names}."
    )
    st.caption(
        "**Sources.** International Monetary Fund, World Economic Outlook — indicator `GGXWDG_NGDP` "
        "(general-government gross debt as % of GDP), retrieved from the IMF DataMapper API. "
        f"Latest reported year per country (mostly {latest_debt_year}). "
        "Dependency ratios from UN World Population Prospects 2024 medium variant. Pension spending "
        f"from OECD PAG (measure `PEP`), reference year {REF_YEAR_SPENDING}.  \n"
        "**Methodology.** Dashed lines mark the OECD medians on each axis; quadrant diagnostics name "
        "countries above median on both axes. One methodology per axis; no cross-country rebasing needed.  \n"
        "**Tools.** IMF DataMapper REST (`https://www.imf.org/external/datamapper/api/v1/GGXWDG_NGDP`), "
        "UN WPP CSV, OECD SDMX CSV, BigQuery joins, Altair bubble scatter.  \n"
        "**Limitations.** Gross (not net) debt — some countries (Japan, Norway) hold large offsetting "
        "sovereign assets. The 2030 WEO value in the tooltip is an IMF projection subject to revision at each WEO release."
    )

st.divider()

# ── Chart 6: Mercer sustainability vs OECD old-age poverty ───────────────────

latest_poverty_year = (
    int(score_outcome["old_age_poverty_rate_year"].max()) if not score_outcome.empty else None
)

st.subheader(
    f"Chart 6 — Mercer sustainability sub-index (2025) vs. measured old-age poverty rate "
    f"({'OECD IDD, latest country-year'})"
)
st.markdown(
    "**Insight.** The Mercer sustainability sub-index rates *system design* — coverage, pre-funding, "
    "contribution levels, demographics. The OECD Income Distribution Database poverty rate measures "
    "the *outcome* — the share of persons aged 65+ living below 50% of national median disposable income. "
    "Plotting one against the other shows where design-quality scores are decoupled from measured old-age "
    "poverty. A sustainable system that still produces widespread old-age poverty suggests sustainability "
    "is being achieved through inadequacy rather than robustness."
)

if score_outcome.empty:
    st.warning(
        "Mercer × OECD IDD data is empty. Ensure both `raw.pc_oecd_old_age_poverty` and "
        "`raw.pc_mercer_index_2025` are loaded."
    )
else:
    so_plot = score_outcome.copy()

    so_scatter = (
        alt.Chart(so_plot)
        .mark_circle(size=160, opacity=0.85)
        .encode(
            x=alt.X(
                "mercer_sustainability:Q",
                title="Mercer sustainability sub-index, 0–100 (higher = better system design)",
                scale=alt.Scale(zero=False, nice=True),
            ),
            y=alt.Y(
                "old_age_poverty_rate_latest:Q",
                title="Persons aged 65+ below 50% of median disposable income, %",
                scale=alt.Scale(zero=True, nice=True),
            ),
            color=alt.Color(
                "region:N",
                title="Region",
                scale=alt.Scale(range=[SKY_BLUE, ORANGE, BLUE_GREEN, VERMILLION, PURPLE]),
            ),
            tooltip=[
                alt.Tooltip("country_name:N", title="Country"),
                alt.Tooltip("mercer_sustainability:Q", title="Mercer sustainability", format=".1f"),
                alt.Tooltip("mercer_overall:Q", title="Mercer overall", format=".1f"),
                alt.Tooltip("old_age_poverty_rate_latest:Q", title="65+ poverty rate %", format=".1f"),
                alt.Tooltip("old_age_poverty_rate_year:Q", title="Poverty year", format="d"),
                alt.Tooltip("net_replacement_rate_latest:Q", title="Net replacement rate %", format=".1f"),
            ],
        )
        .properties(height=500)
    )
    so_labels = (
        alt.Chart(so_plot)
        .mark_text(align="left", dx=8, dy=-4, fontSize=10)
        .encode(
            x="mercer_sustainability:Q",
            y="old_age_poverty_rate_latest:Q",
            text="iso3_code:N",
        )
    )

    trend = (
        alt.Chart(so_plot)
        .transform_regression("mercer_sustainability", "old_age_poverty_rate_latest")
        .mark_line(color=GREY, strokeDash=[4, 4], strokeWidth=1.5)
        .encode(x="mercer_sustainability:Q", y="old_age_poverty_rate_latest:Q")
    )

    st.altair_chart(so_scatter + so_labels + trend, use_container_width=True)

    corr = (
        so_plot[["mercer_sustainability", "old_age_poverty_rate_latest"]].corr().iloc[0, 1]
    )
    worst_outcome = so_plot.sort_values("old_age_poverty_rate_latest", ascending=False).head(1)
    best_outcome = so_plot.sort_values("old_age_poverty_rate_latest", ascending=True).head(1)
    st.markdown(
        f"> Pearson correlation between Mercer sustainability and measured 65+ poverty across "
        f"{len(so_plot)} scored countries: **r = {corr:+.2f}**. "
        f"Highest poverty rate: **{worst_outcome.iloc[0]['country_name']}** "
        f"({worst_outcome.iloc[0]['old_age_poverty_rate_latest']:.1f}%, "
        f"sustainability score {worst_outcome.iloc[0]['mercer_sustainability']:.1f}). "
        f"Lowest: **{best_outcome.iloc[0]['country_name']}** "
        f"({best_outcome.iloc[0]['old_age_poverty_rate_latest']:.1f}%, "
        f"sustainability score {best_outcome.iloc[0]['mercer_sustainability']:.1f})."
    )
    st.caption(
        "**Sources.** Mercer CFA Institute Global Pension Index 2025 (October 2025), sustainability "
        "sub-index (0–100). Old-age poverty rate from OECD Income Distribution Database (dataflow "
        "`DSD_WISE_IDD@DF_IDD`, measure `PR_INC_DISP`, age `Y_GT65`, methodology `METH2012`, "
        "definition `D_CUR`, poverty line `PL_50` = 50% of national median equivalised disposable income). "
        f"Latest reported year per country (range 2020–{latest_poverty_year}).  \n"
        "**Methodology.** Each country's most recent reported poverty rate is used; Mercer score is a "
        "single 2025 value. Dashed line is an OLS regression of poverty on sustainability score. "
        "Both axes share the OECD-38 scope but Mercer covers a subset of countries.  \n"
        "**Tools.** OECD SDMX CSV (IDD dataflow), Mercer seed CSV, BigQuery joins, Altair scatter + "
        "`transform_regression`.  \n"
        "**Limitations.** The OECD relative poverty line is anchored to national median income, so "
        "poverty rates across countries are not purely absolute. Mercer's sustainability methodology is "
        "proprietary and weights system-design factors that are not outcome measures; the two series are "
        "conceptually distinct rather than redundant. Country-year mismatch: most poverty values are 2022 "
        "or 2023 while Mercer scores are 2025."
    )

st.divider()

# ── Methodology ──────────────────────────────────────────────────────────────

st.subheader("Methodology, data vintages, and limitations")
st.markdown(
    f"""
**Scope.** All charts cover the 38 OECD member countries only. This is the cleanest apples-to-apples
comparison available: every indicator is produced by the OECD (or UN for demography) under a single
harmonized methodology.

**Reference-year pinning.** To avoid silent cross-country drift when one country reports through 2024
and another only through 2019, each pension indicator in `staging.pc_pension_system` is pinned to a
single reference year:

| Indicator | Reference year | Reason |
|---|---|---|
| Statutory retirement age | {REF_YEAR_RETIREMENT} | Latest PAG vintage with full OECD-38 coverage |
| Net pension replacement rate | {REF_YEAR_REPLACEMENT} | Same PAG vintage |
| Public pension spending % GDP | {REF_YEAR_SPENDING} | Latest year with full OECD-38 reporting |
| Pension fund assets % GDP | {REF_YEAR_ASSETS} | Latest year with broad coverage |
| Demographic indicators | {latest_observed_year} / {projection_year} | UN WPP 2024 observed + medium-variant projection |

**Data sources.**
- **United Nations World Population Prospects 2024** — released 11 July 2024; historical estimates
  1950–2024 and medium-variant projections 2025–2100.
- **OECD Pensions at a Glance 2023** — published biennially. Dataflows `DSD_PAG@DF_DPS` (retirement
  age, measure `CRPLF22`), `DSD_PAG@DF_PRR` (replacement rates, measure `NPRR100`, optionality `M`),
  `DSD_PAG@DF_PAG` (public pension spending, measure `PEP`).
- **OECD Global Pension Statistics** — dataflow `DSD_FP@DF_PA`, unit `PT_B1GQ`, vehicle type `_T`.
- **Mercer CFA Institute Global Pension Index 2025** — released October 2025 (main report PDF).
- **IMF World Economic Outlook** — indicator `GGXWDG_NGDP` (general-government gross debt, % of GDP),
  retrieved via the IMF DataMapper public API.
- **OECD Income Distribution Database** — dataflow `DSD_WISE_IDD@DF_IDD`, measure `PR_INC_DISP`,
  age `Y_GT65`, poverty line `PL_50` (50% of median equivalised disposable income).

**Pipeline and tools.** Bruin (orchestration) → BigQuery (storage and SQL) → Streamlit + Altair
(interactive dashboard). Raw ingestion lives in `raw.pc_*`; cleaned and pinned tables in `staging.pc_*`;
the per-country mart is `staging.pc_country_pension_profile`. Colour palette is Wong (2011), verified
safe for deuteranopia, protanopia, and tritanopia.

**General limitations.**
- UN WPP medium-variant is a single scenario; high- and low-variant projections diverge substantially
  by 2050.
- OECD indicators reflect statutory / normal parameters, not actual effective retirement age.
- Public pension spending excludes private / occupational pension payouts, which are large in funded
  systems (Netherlands, Denmark).
- Mercer GPI covers 52 countries globally; not all OECD-38 are scored (10 countries — Czechia, Estonia,
  Greece, Hungary, Latvia, Lithuania, Luxembourg, Slovakia, Slovenia, Costa Rica — are absent from
  the 2025 release).
- Correlations reported on charts are point-in-time and do not imply causation.
"""
)
