from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(page_title="The Double Squeeze", layout="wide")

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

# Oil + macro data (monthly grain)
oil = run_raw("""
    SELECT observation_date, brent_crude_usd, cpi_all_yoy, cpi_core_yoy,
           consumer_sentiment, inflation_expectations, crisis_period
    FROM `bruin-playground-arsalan.staging.hormuz_crisis_analysis`
    WHERE observation_date >= '2010-01-01'
    ORDER BY observation_date
""")
oil["observation_date"] = pd.to_datetime(oil["observation_date"])

# Sector-level stock returns (monthly avg close, indexed)
sectors = run_raw("""
    WITH monthly AS (
        SELECT
            sector,
            DATE_TRUNC(date, MONTH) AS month,
            AVG(daily_return_pct) AS avg_daily_return,
            AVG(close) AS avg_close,
            COUNT(*) AS trading_days
        FROM `bruin-playground-arsalan.stock_market_staging.prices_daily`
        WHERE date >= '2010-01-01' AND sector IS NOT NULL
        GROUP BY sector, DATE_TRUNC(date, MONTH)
    )
    SELECT
        sector, month,
        avg_daily_return,
        avg_close,
        trading_days,
        avg_daily_return * trading_days AS monthly_return_pct
    FROM monthly
    ORDER BY sector, month
""")
sectors["month"] = pd.to_datetime(sectors["month"])

# Key individual stocks — oil winners vs consumer losers
stocks = run_raw("""
    WITH monthly AS (
        SELECT
            ticker,
            sector,
            company_name,
            DATE_TRUNC(date, MONTH) AS month,
            AVG(close) AS avg_close,
            AVG(daily_return_pct) AS avg_daily_return,
            COUNT(*) AS trading_days
        FROM `bruin-playground-arsalan.stock_market_staging.prices_daily`
        WHERE date >= '2020-01-01'
          AND ticker IN ('XOM','CVX','COP','OXY','LMT','RTX','NOC',
                         'DAL','UAL','AMZN','WMT','TGT','COST')
        GROUP BY ticker, sector, company_name, DATE_TRUNC(date, MONTH)
    )
    SELECT *, avg_daily_return * trading_days AS monthly_return_pct
    FROM monthly
    ORDER BY ticker, month
""")
stocks["month"] = pd.to_datetime(stocks["month"])

# Wong palette
VERMILLION = "#D55E00"
SKY_BLUE = "#56B4E9"
ORANGE = "#E69F00"
BLUE_GREEN = "#009E73"
BLUE = "#0072B2"
PURPLE = "#CC79A7"
MUTED = "#999999"

# ── Header ────────────────────────────────────────────────────────────

st.title("The Double Squeeze")
st.caption(
    "How oil shocks ripple through the stock market — winners, losers, and the sectors caught in between  ·  "
    "Data: FRED + S&P 500 (FMP)  ·  Pipeline: Bruin + BigQuery"
)
st.markdown("---")

# ══════════════════════════════════════════════════════════════════════
# CHART 1: Oil price vs sector performance — who wins, who loses?
# Scatter: x = Brent monthly change, y = sector monthly return
# One dot per sector-month, colored by sector. Shows clear pattern:
# Energy goes UP when oil goes up, Airlines/Consumer goes DOWN.
# ══════════════════════════════════════════════════════════════════════

st.subheader("When Oil Spikes, Who Wins and Who Loses?")
st.caption(
    "Each dot is one sector in one month. X-axis = Brent crude monthly % change, "
    "Y-axis = sector avg daily stock return that month. "
    "Energy stocks surge when oil spikes. Airlines and consumers get crushed."
)

# Compute monthly Brent change
brent_monthly = oil[oil["brent_crude_usd"].notna()].copy()
brent_monthly["month"] = brent_monthly["observation_date"].dt.to_period("M").dt.to_timestamp()
brent_monthly_avg = brent_monthly.groupby("month")["brent_crude_usd"].mean().reset_index()
brent_monthly_avg["oil_pct_change"] = brent_monthly_avg["brent_crude_usd"].pct_change() * 100

# Join oil change to sector returns
scatter_data = sectors.merge(brent_monthly_avg[["month", "oil_pct_change"]], on="month", how="inner")
scatter_data = scatter_data.dropna(subset=["oil_pct_change", "avg_daily_return"])

# Highlight key sectors
highlight_sectors = ["Energy", "Industrials", "Consumer Discretionary", "Consumer Staples", "Information Technology"]
scatter_filtered = scatter_data[scatter_data["sector"].isin(highlight_sectors)].copy()

sector_colors = {
    "Energy": VERMILLION,
    "Industrials": BLUE,
    "Consumer Discretionary": ORANGE,
    "Consumer Staples": BLUE_GREEN,
    "Information Technology": PURPLE,
}

sel = alt.selection_point(fields=["sector"], bind="legend")

scatter = (
    alt.Chart(scatter_filtered)
    .mark_circle(size=40, opacity=0.5)
    .encode(
        x=alt.X("oil_pct_change:Q", title="Brent crude monthly change (%)",
                 scale=alt.Scale(domain=[-35, 35])),
        y=alt.Y("avg_daily_return:Q", title="Sector avg daily stock return (%)",
                 scale=alt.Scale(domain=[-4, 4])),
        color=alt.Color("sector:N", title="Sector",
                        scale=alt.Scale(domain=list(sector_colors.keys()),
                                        range=list(sector_colors.values())),
                        legend=alt.Legend(orient="top", direction="horizontal")),
        opacity=alt.condition(sel, alt.value(0.6), alt.value(0.05)),
        tooltip=[
            alt.Tooltip("sector:N"),
            alt.Tooltip("month:T", format="%b %Y"),
            alt.Tooltip("oil_pct_change:Q", title="Oil change %", format=".1f"),
            alt.Tooltip("avg_daily_return:Q", title="Stock return %", format=".2f"),
        ],
    )
    .properties(height=450)
    .add_params(sel)
)

# Add regression lines per sector using transform_regression
reg_lines = (
    alt.Chart(scatter_filtered)
    .mark_line(strokeWidth=2.5)
    .encode(
        x="oil_pct_change:Q",
        y="avg_daily_return:Q",
        color=alt.Color("sector:N",
                        scale=alt.Scale(domain=list(sector_colors.keys()),
                                        range=list(sector_colors.values())),
                        legend=None),
        opacity=alt.condition(sel, alt.value(1), alt.value(0.1)),
    )
    .transform_regression("oil_pct_change", "avg_daily_return", groupby=["sector"])
)

zero_x = alt.Chart(pd.DataFrame({"x": [0]})).mark_rule(color="#333", strokeDash=[4, 4]).encode(x="x:Q")
zero_y = alt.Chart(pd.DataFrame({"y": [0]})).mark_rule(color="#333", strokeDash=[4, 4]).encode(y="y:Q")

st.altair_chart(scatter + reg_lines + zero_x + zero_y, use_container_width=True)

# Compute correlations per sector
corr_rows = []
for sector in highlight_sectors:
    sub = scatter_data[scatter_data["sector"] == sector]
    if len(sub) > 20:
        corr = sub["oil_pct_change"].corr(sub["avg_daily_return"])
        corr_rows.append({"sector": sector, "correlation": round(corr, 3)})
corr_df = pd.DataFrame(corr_rows).sort_values("correlation", ascending=False)

corr_strs = [f"**{r['sector']}** ({r['correlation']:+.2f})" for _, r in corr_df.iterrows()]
st.markdown(f"> Oil-stock correlation since 2010: {' · '.join(corr_strs)}")

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════
# CHART 2: The Hormuz Trade — stock price performance since March 1
# Indexed to 100 at March 1, 2026. Oil stocks vs airlines vs consumer.
# ══════════════════════════════════════════════════════════════════════

st.subheader("The Hormuz Trade: Winners vs Losers Since March 1")
st.caption(
    "Stock prices indexed to 100 on March 1, 2026. "
    "Oil & defense stocks surge while airlines and retailers get hammered."
)

# Get daily data for the crisis period
crisis_stocks = run_raw("""
    SELECT
        ticker, company_name, sector, date, close,
        CASE
            WHEN ticker IN ('XOM','CVX','COP','OXY') THEN 'Oil & Gas'
            WHEN ticker IN ('LMT','RTX','NOC') THEN 'Defense'
            WHEN ticker IN ('DAL','UAL') THEN 'Airlines'
            WHEN ticker IN ('AMZN','WMT','TGT','COST') THEN 'Consumer/Retail'
        END AS group_name
    FROM `bruin-playground-arsalan.stock_market_staging.prices_daily`
    WHERE date >= '2026-03-01'
      AND ticker IN ('XOM','CVX','COP','OXY','LMT','RTX','NOC','DAL','UAL','AMZN','WMT','TGT','COST')
    ORDER BY ticker, date
""")
crisis_stocks["date"] = pd.to_datetime(crisis_stocks["date"])

if len(crisis_stocks):
    # Group average by group_name, then index to day 1
    group_avg = crisis_stocks.groupby(["group_name", "date"])["close"].mean().reset_index()

    base_prices = group_avg.groupby("group_name")["close"].first().to_dict()
    group_avg["indexed"] = group_avg.apply(
        lambda r: r["close"] / base_prices[r["group_name"]] * 100, axis=1
    )

    group_colors = {
        "Oil & Gas": VERMILLION,
        "Defense": ORANGE,
        "Airlines": BLUE,
        "Consumer/Retail": BLUE_GREEN,
    }
    group_dashes = {
        "Oil & Gas": [1, 0],
        "Defense": [6, 3],
        "Airlines": [1, 0],
        "Consumer/Retail": [6, 3],
    }
    g_domain = list(group_colors.keys())
    g_range = list(group_colors.values())
    g_dashes = [group_dashes[k] for k in g_domain]

    crisis_chart = (
        alt.Chart(group_avg)
        .mark_line(strokeWidth=3)
        .encode(
            x=alt.X("date:T", title=None),
            y=alt.Y("indexed:Q", title="Price index (Mar 1 = 100)", scale=alt.Scale(zero=False)),
            color=alt.Color("group_name:N", title="Group",
                            scale=alt.Scale(domain=g_domain, range=g_range),
                            legend=alt.Legend(orient="top", direction="horizontal")),
            strokeDash=alt.StrokeDash("group_name:N",
                                       scale=alt.Scale(domain=g_domain, range=g_dashes),
                                       legend=None),
            tooltip=[
                alt.Tooltip("group_name:N", title="Group"),
                alt.Tooltip("date:T", title="Date", format="%b %d"),
                alt.Tooltip("indexed:Q", title="Index", format=".1f"),
            ],
        )
        .properties(height=420)
    )

    baseline = (
        alt.Chart(pd.DataFrame({"y": [100]}))
        .mark_rule(color=MUTED, strokeDash=[4, 4], strokeWidth=1)
        .encode(y="y:Q")
    )

    st.altair_chart(crisis_chart + baseline, use_container_width=True)

    # Show the divergence
    latest_day = group_avg.groupby("group_name")["indexed"].last().sort_values(ascending=False)
    parts = [f"**{name}**: {val:.0f}" for name, val in latest_day.items()]
    spread = latest_day.max() - latest_day.min()
    st.markdown(
        f"> Since March 1: {' · '.join(parts)}. "
        f"The winner-loser spread is **{spread:.0f} points** — "
        f"a massive divergence in under a month."
    )

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════
# CHART 3: Historical crisis returns by sector — heatmap
# For each crisis period, compute total sector return. Show as heatmap.
# Reveals the pattern: Energy always wins, airlines always lose.
# ══════════════════════════════════════════════════════════════════════

st.subheader("The Playbook: Which Sectors Win in Every Oil Crisis?")
st.caption(
    "Average daily stock return by sector during each oil crisis since 2010. "
    "Red = positive (winning), blue = negative (losing). "
    "The pattern is consistent: Energy profits, everyone else pays."
)

crisis_returns = run_raw("""
    WITH crisis_dates AS (
        SELECT observation_date, crisis_period
        FROM `bruin-playground-arsalan.staging.hormuz_crisis_analysis`
        WHERE crisis_period != 'Normal'
          AND observation_date >= '2010-01-01'
    )
    SELECT
        p.sector,
        c.crisis_period,
        ROUND(AVG(p.daily_return_pct), 4) AS avg_daily_return
    FROM `bruin-playground-arsalan.stock_market_staging.prices_daily` p
    INNER JOIN crisis_dates c ON p.date = c.observation_date
    WHERE p.sector IS NOT NULL
    GROUP BY p.sector, c.crisis_period
    ORDER BY p.sector, c.crisis_period
""")

if len(crisis_returns):
    # Order crises chronologically
    crisis_order = ["2011 Arab Spring", "2014 Oil Glut", "2020 COVID Crash",
                    "2022 Russia-Ukraine", "2026 Hormuz Crisis"]
    crisis_returns = crisis_returns[crisis_returns["crisis_period"].isin(crisis_order)]

    # Order sectors by avg return during crises (most positive first)
    sector_order = (
        crisis_returns.groupby("sector")["avg_daily_return"]
        .mean().sort_values(ascending=False).index.tolist()
    )

    heatmap = (
        alt.Chart(crisis_returns)
        .mark_rect(cornerRadius=4)
        .encode(
            x=alt.X("crisis_period:N", title=None,
                     sort=crisis_order,
                     axis=alt.Axis(labelAngle=-30)),
            y=alt.Y("sector:N", title=None, sort=sector_order),
            color=alt.Color("avg_daily_return:Q", title="Avg daily return %",
                            scale=alt.Scale(scheme="blueorange", domainMid=0)),
            tooltip=[
                alt.Tooltip("sector:N"),
                alt.Tooltip("crisis_period:N", title="Crisis"),
                alt.Tooltip("avg_daily_return:Q", title="Avg daily return %", format=".3f"),
            ],
        )
        .properties(height=400)
    )

    text = (
        alt.Chart(crisis_returns)
        .mark_text(fontSize=11, fontWeight="bold")
        .encode(
            x=alt.X("crisis_period:N", sort=crisis_order),
            y=alt.Y("sector:N", sort=sector_order),
            text=alt.Text("avg_daily_return:Q", format=".2f"),
            color=alt.condition(
                (alt.datum.avg_daily_return > 0.15) | (alt.datum.avg_daily_return < -0.15),
                alt.value("white"),
                alt.value("black"),
            ),
        )
    )

    st.altair_chart(heatmap + text, use_container_width=True)

    # Energy row callout
    energy = crisis_returns[crisis_returns["sector"] == "Energy"]
    if len(energy):
        wins = (energy["avg_daily_return"] > 0).sum()
        st.markdown(
            f"> Energy stocks posted **positive returns in {wins} of {len(energy)} oil crises**. "
            f"The playbook is clear: when oil spikes, buy energy, sell airlines."
        )

st.markdown("---")
st.caption(
    "Data: FRED + S&P 500 via FMP · Colorblind-safe: Wong 2011 + blueorange diverging · "
    "Pipeline: Bruin + BigQuery + Altair"
)
