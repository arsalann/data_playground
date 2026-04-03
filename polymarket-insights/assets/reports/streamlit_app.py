import altair as alt
import pandas as pd
import streamlit as st
import yfinance as yf
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(page_title="Polymarket vs Financial Markets", layout="wide")

PROJECT_ID = "bruin-playground-arsalan"

VERMILLION = "#D55E00"
SKY_BLUE = "#56B4E9"
ORANGE = "#E69F00"
BLUISH_GREEN = "#009E73"
BLUE = "#0072B2"
YELLOW = "#F0E442"
REDDISH_PURPLE = "#CC79A7"
GREY = "#999999"
BLACK = "#000000"


@st.cache_resource
def get_client():
    creds = service_account.Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(project=PROJECT_ID, credentials=creds)


def q(sql: str) -> pd.DataFrame:
    return get_client().query(sql).to_dataframe()


def prep(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"])
    return out


# ── Load data ────────────────────────────────────────────────────────

khamenei = q("WITH m AS (SELECT question FROM `bruin-playground-arsalan.staging.polymarket_markets_enriched` WHERE LOWER(question) LIKE '%khamenei out%' AND LOWER(question) LIKE '%february 28%' ORDER BY volume_total DESC LIMIT 1) SELECT DATE(ph.timestamp) as date, ROUND(ph.price*100,1) as pct FROM `bruin-playground-arsalan.raw.polymarket_price_history` ph JOIN m ON ph.question=m.question WHERE ph.outcome_label='Yes' AND ph.timestamp>='2026-01-15' ORDER BY ph.timestamp")

strikes = q("WITH m AS (SELECT question FROM `bruin-playground-arsalan.staging.polymarket_markets_enriched` WHERE LOWER(question) LIKE '%us strikes iran%' AND LOWER(question) LIKE '%february 28%' ORDER BY volume_total DESC LIMIT 1) SELECT DATE(ph.timestamp) as date, ROUND(ph.price*100,1) as pct FROM `bruin-playground-arsalan.raw.polymarket_price_history` ph JOIN m ON ph.question=m.question WHERE ph.outcome_label='Yes' AND ph.timestamp>='2026-01-15' ORDER BY ph.timestamp")

try:
    oil = q("SELECT observation_date as date, brent_crude_usd FROM `bruin-playground-arsalan.staging.hormuz_prices_wide` WHERE observation_date>='2026-01-15' AND observation_date<='2026-03-15' AND brent_crude_usd IS NOT NULL ORDER BY observation_date")
except Exception:
    oil = pd.DataFrame()

try:
    equities = q("SELECT date, ticker, adj_close FROM `bruin-playground-arsalan.stock_market_staging.prices_daily` WHERE ticker IN ('XOM','LMT') AND date>='2026-01-15' AND date<='2026-03-15' ORDER BY date, ticker")
except Exception:
    equities = pd.DataFrame()

@st.cache_data(ttl=3600)
def fetch_yfinance(ticker, start, end):
    df = yf.download(ticker, start=start, end=end, progress=False)
    if df.empty:
        return pd.DataFrame()
    df = df[["Close"]].reset_index()
    df.columns = ["date", "close"]
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    return df

gold = fetch_yfinance("GC=F", "2026-01-15", "2026-03-16")
bitcoin = fetch_yfinance("BTC-USD", "2026-01-15", "2026-03-16")

# ── Header ───────────────────────────────────────────────────────────

st.title("Polymarket vs Financial Markets")
st.markdown("Q1 2026 prediction market probabilities overlaid with commodity, equity, and crypto prices.")
st.markdown("---")

# ── Chart: Khamenei Ouster — Cross-Market Response ───────────────────

st.header("Khamenei Ouster: Cross-Market Response")
st.markdown(
    "Asset prices indexed to 100 at their starting value, overlaid with "
    "Polymarket probabilities for the Khamenei ouster (dashed red) "
    "and US strikes on Iran (dashed black) on the right axis."
)

has_financial = not oil.empty or not equities.empty or not gold.empty or not bitcoin.empty

if not khamenei.empty and has_financial:
    # Build indexed financial series
    parts = []

    def add_series(df, value_col, label):
        d = df.copy()
        d["date"] = pd.to_datetime(d["date"])
        base = d[value_col].iloc[0]
        if base and base != 0:
            d["idx"] = (d[value_col] / base) * 100
            d["series"] = label
            parts.append(d[["date", "idx", "series"]])

    if not oil.empty:
        add_series(prep(oil), "brent_crude_usd", "Brent crude")

    if not equities.empty:
        eq = prep(equities)
        for ticker, label in [("XOM", "ExxonMobil"), ("LMT", "Lockheed Martin")]:
            t = eq[eq["ticker"] == ticker][["date", "adj_close"]].copy()
            if not t.empty:
                add_series(t, "adj_close", label)

    if not gold.empty:
        add_series(gold, "close", "Gold")

    if not bitcoin.empty:
        add_series(bitcoin, "close", "Bitcoin")

    if parts:
        fin = pd.concat(parts, ignore_index=True)
        fin["axis"] = "price"

        # Add probability series into the same dataframe
        kp = prep(khamenei); kp["series"] = "Khamenei ousted (prob)"; kp["idx"] = kp["pct"]; kp["axis"] = "prob"
        sp = prep(strikes);  sp["series"] = "US strikes Iran (prob)"; sp["idx"] = sp["pct"]; sp["axis"] = "prob"
        all_data = pd.concat([fin, kp[["date", "idx", "series", "axis"]], sp[["date", "idx", "series", "axis"]]], ignore_index=True)

        all_names = all_data["series"].unique().tolist()
        cmap = {
            "Brent crude": "#654321",
            "ExxonMobil": BLUE,
            "Lockheed Martin": BLUISH_GREEN,
            "Gold": YELLOW,
            "Bitcoin": REDDISH_PURPLE,
            "Khamenei ousted (prob)": VERMILLION,
            "US strikes Iran (prob)": BLACK,
        }
        dash_map = {s: [1, 0] for s in all_names}  # solid by default
        dash_map["Khamenei ousted (prob)"] = [6, 4]
        dash_map["US strikes Iran (prob)"] = [6, 4]

        # Price layer (left axis)
        price_data = all_data[all_data["axis"] == "price"]
        price_names = price_data["series"].unique().tolist()
        price_layer = alt.Chart(price_data).mark_line(strokeWidth=2).encode(
            x=alt.X("date:T", axis=alt.Axis(format="%b %d")),
            y=alt.Y("idx:Q", title="Price index (100 = start)", scale=alt.Scale(domain=[50, 150])),
            color=alt.Color("series:N", scale=alt.Scale(
                domain=all_names, range=[cmap.get(s, GREY) for s in all_names],
            ), legend=alt.Legend(title=None, orient="top")),
            strokeDash=alt.StrokeDash("series:N", scale=alt.Scale(
                domain=all_names, range=[dash_map.get(s, [1, 0]) for s in all_names],
            ), legend=None),
        )

        # Probability layer (right axis)
        prob_data = all_data[all_data["axis"] == "prob"]
        prob_layer = alt.Chart(prob_data).mark_line(strokeWidth=2).encode(
            x="date:T",
            y=alt.Y("idx:Q", title="Probability (%)"),
            color=alt.Color("series:N", scale=alt.Scale(
                domain=all_names, range=[cmap.get(s, GREY) for s in all_names],
            ), legend=None),
            strokeDash=alt.StrokeDash("series:N", scale=alt.Scale(
                domain=all_names, range=[dash_map.get(s, [1, 0]) for s in all_names],
            ), legend=None),
        )

        chart = alt.layer(price_layer, prob_layer).resolve_scale(
            y="independent"
        ).properties(height=450)
        st.altair_chart(chart, use_container_width=True)

elif not has_financial:
    st.info("No financial data available.")

st.markdown("---")
st.markdown(
    "**Sources:** "
    "[Polymarket](https://polymarket.com) prediction market APIs (Gamma & CLOB), "
    "[FRED](https://fred.stlouisfed.org) oil prices (Brent crude), "
    "[Yahoo Finance](https://finance.yahoo.com) (gold futures, Bitcoin), "
    "S&P 500 equity prices via [yfinance](https://github.com/ranaroussi/yfinance) "
    "and [Financial Modeling Prep](https://financialmodelingprep.com)."
)
st.markdown(
    "**Data stack:** "
    "[Bruin](https://github.com/bruin-data/bruin) (pipeline orchestration), "
    "Google BigQuery (warehouse), "
    "[Streamlit](https://streamlit.io) (dashboard), "
    "[Altair](https://altair-viz.github.io) (visualization)."
)
