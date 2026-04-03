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

st.title("Prediction Markets and Financial Data")
st.markdown("Q1 2026 Polymarket probabilities alongside commodity, equity, and crypto prices.")
st.markdown(
    '<small><b>Sources:</b> '
    '<a href="https://gamma-api.polymarket.com/markets">Polymarket Gamma API</a> &amp; '
    '<a href="https://clob.polymarket.com/prices-history">CLOB API</a> (prediction markets), '
    '<a href="https://fred.stlouisfed.org/series/DCOILBRENTEU">FRED DCOILBRENTEU</a> (Brent crude), '
    '<a href="https://finance.yahoo.com/quote/GC=F">Yahoo Finance GC=F</a> (gold futures), '
    '<a href="https://finance.yahoo.com/quote/BTC-USD">Yahoo Finance BTC-USD</a> (Bitcoin), '
    '<a href="https://financialmodelingprep.com/api/v3/historical-price-full/XOM">FMP</a> (equities).<br>'
    '<b>Data stack:</b> '
    '<a href="https://github.com/bruin-data/bruin">Bruin</a> (pipeline orchestration), '
    'Google BigQuery (warehouse), '
    '<a href="https://streamlit.io">Streamlit</a> (dashboard), '
    '<a href="https://altair-viz.github.io">Altair</a> (visualization).</small>',
    unsafe_allow_html=True,
)
st.markdown("---")

# ── Chart: Khamenei Ouster — Cross-Market Response ───────────────────

st.header("Iran Crisis: Prediction Probabilities and Asset Prices")
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

# ── Chart 2: Daily Returns Around the Event ──────────────────────────

st.header("Daily Returns Around the Event")
st.markdown(
    "Daily percentage change for each asset from Feb 24 to Mar 13. "
    "The Khamenei ouster resolved on Mar 1; the largest single-day moves followed immediately."
)

if has_financial:
    # Build daily returns for each asset
    ret_parts = []
    cmap_ret = {
        "Brent crude": "#654321",
        "ExxonMobil": BLUE,
        "Lockheed Martin": BLUISH_GREEN,
        "Gold": YELLOW,
        "Bitcoin": REDDISH_PURPLE,
    }

    def add_returns(df, value_col, label):
        d = df.copy()
        d["date"] = pd.to_datetime(d["date"])
        d = d.sort_values("date")
        d["daily_ret"] = d[value_col].pct_change() * 100
        d["series"] = label
        ret_parts.append(d[["date", "daily_ret", "series"]].dropna())

    if not oil.empty:
        add_returns(prep(oil), "brent_crude_usd", "Brent crude")
    if not equities.empty:
        eq = prep(equities)
        for ticker, label in [("XOM", "ExxonMobil"), ("LMT", "Lockheed Martin")]:
            t = eq[eq["ticker"] == ticker][["date", "adj_close"]].copy()
            if not t.empty:
                add_returns(t, "adj_close", label)
    if not gold.empty:
        add_returns(gold, "close", "Gold")
    if not bitcoin.empty:
        add_returns(bitcoin, "close", "Bitcoin")

    if ret_parts:
        rets = pd.concat(ret_parts, ignore_index=True)
        # Focus on the event window
        rets = rets[(rets["date"] >= "2026-02-24") & (rets["date"] <= "2026-03-13")]
        ret_names = rets["series"].unique().tolist()

        bars = alt.Chart(rets).mark_bar().encode(
            x=alt.X("date:T", axis=alt.Axis(format="%b %d")),
            y=alt.Y("daily_ret:Q", title="Daily return (%)"),
            color=alt.Color("series:N", scale=alt.Scale(
                domain=ret_names, range=[cmap_ret.get(s, GREY) for s in ret_names],
            ), legend=alt.Legend(title=None, orient="top")),
            xOffset="series:N",
        )

        zero = alt.Chart(pd.DataFrame({"y": [0]})).mark_rule(color=GREY).encode(y="y:Q")

        st.altair_chart((zero + bars).properties(height=350), use_container_width=True)

st.markdown("---")

# ── Chart 3: Post-Event Cumulative Returns ───────────────────────────

st.header("Post-Event Cumulative Returns (from Mar 1)")
st.markdown(
    "Asset prices re-indexed to 100 on March 1, the day the Khamenei ouster resolved. "
    "Shows how each asset performed in the two weeks after the event."
)

if has_financial:
    post_parts = []

    def add_post_series(df, value_col, label):
        d = df.copy()
        d["date"] = pd.to_datetime(d["date"])
        d = d[d["date"] >= "2026-03-01"].sort_values("date")
        if len(d) >= 2:
            base = d[value_col].iloc[0]
            if base and base != 0:
                d["idx"] = (d[value_col] / base) * 100
                d["series"] = label
                post_parts.append(d[["date", "idx", "series"]])

    if not oil.empty:
        add_post_series(prep(oil), "brent_crude_usd", "Brent crude")
    if not equities.empty:
        eq = prep(equities)
        for ticker, label in [("XOM", "ExxonMobil"), ("LMT", "Lockheed Martin")]:
            t = eq[eq["ticker"] == ticker][["date", "adj_close"]].copy()
            if not t.empty:
                add_post_series(t, "adj_close", label)
    if not gold.empty:
        add_post_series(gold, "close", "Gold")
    if not bitcoin.empty:
        add_post_series(bitcoin, "close", "Bitcoin")

    if post_parts:
        post = pd.concat(post_parts, ignore_index=True)
        post_names = post["series"].unique().tolist()

        lines = alt.Chart(post).mark_line(strokeWidth=2).encode(
            x=alt.X("date:T", axis=alt.Axis(format="%b %d")),
            y=alt.Y("idx:Q", title="Price index (100 = Mar 1)"),
            color=alt.Color("series:N", scale=alt.Scale(
                domain=post_names, range=[cmap_ret.get(s, GREY) for s in post_names],
            ), legend=alt.Legend(title=None, orient="top")),
        )

        baseline = alt.Chart(pd.DataFrame({"y": [100]})).mark_rule(
            strokeDash=[4, 4], color=GREY
        ).encode(y="y:Q")

        st.altair_chart((baseline + lines).properties(height=380), use_container_width=True)

st.markdown("---")

# ── Chart 4: Total Return Before vs After ────────────────────────────

st.header("Total Return: Two Weeks Before vs After")
st.markdown(
    "Percentage return for each asset in the 14 calendar days before and after March 1. "
)

if has_financial:
    summary_rows = []

    def compute_period_return(df, value_col, label):
        d = df.copy()
        d["date"] = pd.to_datetime(d["date"])
        d = d.sort_values("date")

        pre = d[(d["date"] >= "2026-02-15") & (d["date"] < "2026-03-01")]
        post = d[(d["date"] >= "2026-03-01") & (d["date"] <= "2026-03-15")]

        if len(pre) >= 2:
            pre_ret = (pre[value_col].iloc[-1] / pre[value_col].iloc[0] - 1) * 100
            summary_rows.append({"asset": label, "period": "Before (Feb 15 – Feb 28)", "return_pct": round(pre_ret, 1)})
        if len(post) >= 2:
            post_ret = (post[value_col].iloc[-1] / post[value_col].iloc[0] - 1) * 100
            summary_rows.append({"asset": label, "period": "After (Mar 1 – Mar 13)", "return_pct": round(post_ret, 1)})

    if not oil.empty:
        compute_period_return(prep(oil), "brent_crude_usd", "Brent crude")
    if not equities.empty:
        eq = prep(equities)
        for ticker, label in [("XOM", "ExxonMobil"), ("LMT", "Lockheed Martin")]:
            t = eq[eq["ticker"] == ticker][["date", "adj_close"]].copy()
            if not t.empty:
                compute_period_return(t, "adj_close", label)
    if not gold.empty:
        compute_period_return(gold, "close", "Gold")
    if not bitcoin.empty:
        compute_period_return(bitcoin, "close", "Bitcoin")

    if summary_rows:
        summary = pd.DataFrame(summary_rows)

        bars = alt.Chart(summary).mark_bar().encode(
            x=alt.X("return_pct:Q", title="Total return (%)"),
            y=alt.Y("asset:N", title=None, sort="-x"),
            color=alt.Color("period:N", scale=alt.Scale(
                domain=["Before (Feb 15 – Feb 28)", "After (Mar 1 – Mar 13)"],
                range=[GREY, VERMILLION],
            ), legend=alt.Legend(title=None, orient="top")),
            xOffset="period:N",
        )

        zero = alt.Chart(pd.DataFrame({"x": [0]})).mark_rule(color=BLACK).encode(x="x:Q")

        st.altair_chart((zero + bars).properties(height=250), use_container_width=True)

