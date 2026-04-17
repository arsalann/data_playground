from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(page_title="AI Energy Paradox", layout="wide")

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


# ── Wong 2011 palette ────────────────────────────────────────────────
BLUE_GREEN = "#009E73"
MUTED = "#999999"

# ── Load data ────────────────────────────────────────────────────────

global_elec = run_raw("""
    SELECT year, renewable_share_pct, coal_share_pct
    FROM `bruin-playground-arsalan.staging.aep_electricity_by_source`
    WHERE LOWER(country_or_area) = 'world'
      AND total_generation_twh IS NOT NULL
    ORDER BY year
""")

countries = run_raw("""
    SELECT country, country_code, year, renewable_share_pct,
           emissions_intensity_gco2kwh, total_generation_twh,
           dc_share_of_electricity_pct, paradox_category
    FROM `bruin-playground-arsalan.staging.aep_country_paradox`
    ORDER BY renewable_share_pct DESC
""")

# ── Header ───────────────────────────────────────────────────────────

st.title("AI Energy Paradox")
st.caption(
    "Data: OWID (Ember), IEA, CBS Netherlands, Borderstep, Oxford Economics, "
    "Ember, Wood Mackenzie  ·  Pipeline: Bruin + BigQuery"
)

# ══════════════════════════════════════════════════════════════════════
# CHART 1: Global Renewable vs Coal Generation Share
# ══════════════════════════════════════════════════════════════════════

st.subheader("Global Renewable vs Coal Generation Share, 2000–2024")
st.caption(
    "Share of global electricity generation (%) by source. "
    "Source: Our World in Data / Ember."
)

if len(global_elec):
    ge = global_elec[global_elec["year"] >= 2000].copy()
    shares = ge[["year", "renewable_share_pct", "coal_share_pct"]].melt(
        id_vars=["year"], var_name="source_col", value_name="share_pct"
    )
    shares = shares[shares["share_pct"].notna()]

    label_map = {"renewable_share_pct": "Renewables", "coal_share_pct": "Coal"}
    shares["source"] = shares["source_col"].map(label_map)
    color_map = {"Renewables": BLUE_GREEN, "Coal": MUTED}

    sel = alt.selection_point(fields=["source"], bind="legend")

    lines = (
        alt.Chart(shares)
        .mark_line(strokeWidth=3)
        .encode(
            x=alt.X("year:O", title="Year", axis=alt.Axis(values=[2000, 2005, 2010, 2015, 2020, 2024])),
            y=alt.Y("share_pct:Q", title="Share of global generation (%)", scale=alt.Scale(domain=[15, 45])),
            color=alt.Color(
                "source:N", title="Source",
                scale=alt.Scale(domain=list(color_map.keys()), range=list(color_map.values())),
                legend=alt.Legend(orient="top"),
            ),
            strokeDash=alt.StrokeDash(
                "source:N",
                scale=alt.Scale(domain=["Renewables", "Coal"], range=[[1, 0], [6, 3]]),
                legend=None,
            ),
            opacity=alt.condition(sel, alt.value(1), alt.value(0.2)),
            tooltip=[
                alt.Tooltip("year:O", title="Year"),
                alt.Tooltip("source:N"),
                alt.Tooltip("share_pct:Q", title="Share %", format=".1f"),
            ],
        )
        .properties(height=380)
        .add_params(sel)
    )

    st.altair_chart(lines, use_container_width=True)

    latest_r = ge.iloc[-1]
    gap = latest_r["coal_share_pct"] - latest_r["renewable_share_pct"]
    early = ge[ge["year"] == 2000]
    if len(early):
        early_gap = early.iloc[0]["coal_share_pct"] - early.iloc[0]["renewable_share_pct"]
        st.markdown(
            f"> In {int(latest_r['year'])}, renewables reached **{latest_r['renewable_share_pct']:.1f}%** "
            f"of global generation vs coal at **{latest_r['coal_share_pct']:.1f}%** — "
            f"a gap of **{gap:.1f}pp**, down from {early_gap:.0f}pp in 2000."
        )

    st.markdown(
        '<span style="color: grey; font-size: 0.8em;">'
        "Source: Our World in Data / Ember (CC-BY)  ·  "
        "Tools: Bruin, BigQuery, Streamlit, Altair"
        "</span>",
        unsafe_allow_html=True,
    )

st.divider()

# ══════════════════════════════════════════════════════════════════════
# CHART 2: Data Center Share of National Electricity by Country
# ══════════════════════════════════════════════════════════════════════

st.subheader("Data Center Share of National Electricity by Country")
st.caption(
    "These are the only 9 countries with published data center electricity share figures "
    "(from IEA, CBS Netherlands, Borderstep, Oxford Economics, Ember, Wood Mackenzie). "
    "Bar color shows grid carbon intensity (gCO2/kWh)."
)

if len(countries):
    has_dc = countries[countries["dc_share_of_electricity_pct"].notna()].copy()

    if len(has_dc):
        bars = (
            alt.Chart(has_dc)
            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X("dc_share_of_electricity_pct:Q", title="DC share of national electricity (%)"),
                y=alt.Y("country:N", title=None, sort="-x"),
                color=alt.Color(
                    "emissions_intensity_gco2kwh:Q",
                    title="Grid intensity (gCO2/kWh)",
                    scale=alt.Scale(scheme="viridis", reverse=True, domain=[0, 600]),
                    legend=alt.Legend(orient="top", gradientLength=300),
                ),
                tooltip=[
                    alt.Tooltip("country:N"),
                    alt.Tooltip("dc_share_of_electricity_pct:Q", title="DC share %", format=".1f"),
                    alt.Tooltip("renewable_share_pct:Q", title="Renewables %", format=".1f"),
                    alt.Tooltip("emissions_intensity_gco2kwh:Q", title="Grid gCO2/kWh", format=".0f"),
                    alt.Tooltip("total_generation_twh:Q", title="Total gen TWh", format=",.0f"),
                ],
            )
            .properties(height=380)
        )

        text_labels = (
            alt.Chart(has_dc)
            .mark_text(dx=5, align="left", fontSize=12, fontWeight="bold")
            .encode(
                x=alt.X("dc_share_of_electricity_pct:Q"),
                y=alt.Y("country:N", sort="-x"),
                text=alt.Text("dc_share_of_electricity_pct:Q", format=".1f"),
                color=alt.value("#333"),
            )
        )

        st.altair_chart(bars + text_labels, use_container_width=True)

        # Context table
        display_cols = ["country", "dc_share_of_electricity_pct", "renewable_share_pct",
                        "emissions_intensity_gco2kwh", "total_generation_twh", "paradox_category"]
        rename = {
            "country": "Country",
            "dc_share_of_electricity_pct": "DC Share %",
            "renewable_share_pct": "Renewable %",
            "emissions_intensity_gco2kwh": "Grid gCO2/kWh",
            "total_generation_twh": "Total Gen TWh",
            "paradox_category": "Category",
        }
        st.dataframe(
            has_dc[display_cols].sort_values("dc_share_of_electricity_pct", ascending=False).rename(columns=rename),
            hide_index=True,
        )

        st.markdown(
            '<span style="color: grey; font-size: 0.8em;">'
            "Sources: IEA (Ireland, Singapore, US, China), CBS Netherlands, "
            "Borderstep Institute (Germany), Oxford Economics (UK), "
            "Ember (France), Wood Mackenzie (Japan)  ·  "
            "Grid intensity from OWID / Ember  ·  "
            "Tools: Bruin, BigQuery, Streamlit, Altair"
            "</span>",
            unsafe_allow_html=True,
        )
    else:
        st.info("No country-level data center electricity share data available.")

st.divider()

# ── Methodology ──────────────────────────────────────────────────────

with st.expander("Methodology and data sources"):
    st.markdown("""
**Chart 1 — Global Renewable vs Coal Generation Share**
- **Data:** Our World in Data Energy Dataset, which incorporates Ember's Yearly Electricity Data (CC-BY). Covers 200+ countries from 2000 onward.
- **Method:** Renewable and coal shares are each source's generation (TWh) divided by total global generation, as reported by OWID/Ember.

**Chart 2 — Data Center Share of National Electricity by Country**
- **Ireland (21%):** IEA "Energy and AI" report, Jan 2025. Based on EirGrid metered data.
- **Singapore (7%):** IEA "Energy and AI" report, Jan 2025.
- **Netherlands (4.6%):** CBS (Statistics Netherlands), Dec 2025. Official metered electricity.
- **Germany (4%):** Borderstep Institute, 2025. Modeled from facility-level data.
- **United States (4%):** IEA "Energy and AI" report, Jan 2025.
- **United Kingdom (2.6%):** Oxford Economics, 2025.
- **France (2%):** Ember "Grids for Data Centres", Jun 2025.
- **Japan (2%):** Wood Mackenzie, 2025.
- **China (1.1%):** Derived from IEA estimate of ~100 TWh DC demand / 9,800 TWh national generation.
- **Grid carbon intensity:** OWID / Ember (gCO2 per kWh of electricity generated).

**Notes**
- DC share figures come from different organizations using different methodologies. Direct cross-country comparison should be made with caution.
- These are the only 9 countries with published data center electricity share estimates as of early 2026.

**Tools:** Bruin (pipeline orchestration), BigQuery (data warehouse), Streamlit + Altair (visualization). Colorblind-safe: Wong 2011 palette + Viridis scale.
""")
