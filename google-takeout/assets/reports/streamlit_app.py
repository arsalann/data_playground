from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(
    page_title="Google Search vs ChatGPT",
    layout="wide",
)

PROJECT_ID = "bruin-playground-arsalan"
CHATGPT_LAUNCH = pd.Timestamp("2022-11-30")

base_path = Path(__file__).parent


@st.cache_resource
def get_client():
    credentials = service_account.Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)


def run_query(filename: str) -> pd.DataFrame:
    sql = (base_path / filename).read_text()
    return get_client().query(sql).to_dataframe()


# ──────────────────────────────────────────────────────────────────────
# Load data
# ──────────────────────────────────────────────────────────────────────

monthly = run_query("searches_monthly.sql")
daily = run_query("searches_daily.sql")

monthly["month"] = pd.to_datetime(monthly["month"])
daily["search_date"] = pd.to_datetime(daily["search_date"])

monthly = monthly.sort_values("month")
daily = daily.sort_values("search_date")

HIGHLIGHT = "#D55E00"
DEFAULT = "#56B4E9"
MUTED = "#999999"

ERA_DOMAIN = ["Pre-ChatGPT", "Post-ChatGPT"]
ERA_RANGE = [DEFAULT, HIGHLIGHT]


def format_number(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{value:,.0f}"


def format_pct(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{value:+.1f}%"


# ──────────────────────────────────────────────────────────────────────
# Header
# ──────────────────────────────────────────────────────────────────────

st.title("Did ChatGPT Change My Google Searches?")
st.caption("Google Takeout search history, grouped by month and day.")

if len(monthly):
    date_min = monthly["month"].min().strftime("%b %Y")
    date_max = monthly["month"].max().strftime("%b %Y")
    st.info(f"Data range: **{date_min}** to **{date_max}**.")

st.markdown("---")

# ──────────────────────────────────────────────────────────────────────
# KPIs
# ──────────────────────────────────────────────────────────────────────

total_searches = int(daily["search_count"].sum()) if len(daily) else None
pre_monthly = monthly[~monthly["is_post_chatgpt"]]
post_monthly = monthly[monthly["is_post_chatgpt"]]

pre_avg = pre_monthly["search_count"].mean() if len(pre_monthly) else None
post_avg = post_monthly["search_count"].mean() if len(post_monthly) else None

change_pct = None
if (
    pre_avg is not None
    and post_avg is not None
    and pd.notna(pre_avg)
    and pd.notna(post_avg)
    and pre_avg != 0
):
    change_pct = (post_avg - pre_avg) / pre_avg * 100

latest_month = monthly["month"].max().strftime("%b %Y") if len(monthly) else "n/a"

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Searches", format_number(total_searches))
with col2:
    st.metric("Latest Month", format_number(post_monthly["search_count"].iloc[-1] if len(post_monthly) else None), latest_month)
with col3:
    st.metric("Pre-ChatGPT Avg", f"{format_number(pre_avg)}/mo")
with col4:
    delta_label = format_pct(change_pct) if change_pct is not None else None
    st.metric("Post-ChatGPT Avg", f"{format_number(post_avg)}/mo", delta_label)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════
# 1. Monthly Searches
# ══════════════════════════════════════════════════════════════════════

st.subheader("Monthly Search Volume")
st.caption(
    "Searches per month. The vertical line marks Nov 30, 2022, "
    "when ChatGPT launched."
)

monthly_bars = (
    alt.Chart(monthly)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    .encode(
        x=alt.X("month:T", title="Month"),
        y=alt.Y("search_count:Q", title="Searches"),
        color=alt.Color(
            "era:N",
            title="Era",
            scale=alt.Scale(domain=ERA_DOMAIN, range=ERA_RANGE),
        ),
        tooltip=[
            alt.Tooltip("month:T", title="Month", format="%b %Y"),
            alt.Tooltip("search_count:Q", title="Searches", format=","),
            alt.Tooltip("era:N", title="Era"),
        ],
    )
    .properties(height=380)
)

chatgpt_rule = (
    alt.Chart(pd.DataFrame({"x": [CHATGPT_LAUNCH]}))
    .mark_rule(color="#333333", strokeDash=[6, 3], strokeWidth=2)
    .encode(x="x:T")
)
chatgpt_label = (
    alt.Chart(pd.DataFrame({"x": [CHATGPT_LAUNCH], "label": ["ChatGPT launch"]}))
    .mark_text(align="right", dx=-8, dy=-10, fontSize=11, fontWeight="bold", color="#333333")
    .encode(x="x:T", text="label:N")
)

layered = monthly_bars + chatgpt_rule + chatgpt_label

if pre_avg is not None and not pd.isna(pre_avg):
    avg_rule = (
        alt.Chart(pd.DataFrame({"avg": [pre_avg]}))
        .mark_rule(color=MUTED, strokeDash=[6, 3], strokeWidth=1.5)
        .encode(y="avg:Q")
    )
    avg_text = (
        alt.Chart(pd.DataFrame({"avg": [pre_avg], "label": [f"Pre-ChatGPT avg: {pre_avg:,.0f}/mo"]}))
        .mark_text(align="left", dx=5, dy=-8, fontSize=11, color=MUTED)
        .encode(y="avg:Q", text="label:N")
    )
    layered += avg_rule + avg_text

st.altair_chart(layered, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════
# 2. Daily Trend (Rolling)
# ══════════════════════════════════════════════════════════════════════

st.markdown("---")
st.subheader("Daily Searches (28-day rolling average)")

daily_rolling = daily.copy()
daily_rolling["rolling_28d"] = daily_rolling["search_count"].rolling(
    window=28, min_periods=7
).mean()
daily_rolling = daily_rolling[daily_rolling["rolling_28d"].notna()]

trend_line = (
    alt.Chart(daily_rolling)
    .mark_line(strokeWidth=2.5)
    .encode(
        x=alt.X("search_date:T", title="Date"),
        y=alt.Y("rolling_28d:Q", title="Searches (28d avg)", scale=alt.Scale(zero=False)),
        color=alt.Color(
            "era:N",
            title="Era",
            scale=alt.Scale(domain=ERA_DOMAIN, range=ERA_RANGE),
        ),
        tooltip=[
            alt.Tooltip("search_date:T", title="Date", format="%b %d, %Y"),
            alt.Tooltip("rolling_28d:Q", title="28d avg", format=",.1f"),
            alt.Tooltip("era:N", title="Era"),
        ],
    )
    .properties(height=340)
)

st.altair_chart(trend_line + chatgpt_rule, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════
# 3. Day of Week Pattern
# ══════════════════════════════════════════════════════════════════════

st.markdown("---")
st.subheader("Average Searches by Day of Week")

weekday = (
    daily.groupby(["day_of_week", "day_name", "era"], as_index=False)
    .agg(avg_searches=("search_count", "mean"))
)
day_order = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
weekday["day_name"] = pd.Categorical(weekday["day_name"], categories=day_order, ordered=True)
weekday = weekday.sort_values("day_name")

weekday_bars = (
    alt.Chart(weekday)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    .encode(
        x=alt.X("day_name:N", title="Day of Week", sort=day_order),
        y=alt.Y("avg_searches:Q", title="Avg Searches per Day"),
        color=alt.Color(
            "era:N",
            title="Era",
            scale=alt.Scale(domain=ERA_DOMAIN, range=ERA_RANGE),
        ),
        tooltip=[
            alt.Tooltip("day_name:N", title="Day"),
            alt.Tooltip("avg_searches:Q", title="Avg Searches", format=",.1f"),
            alt.Tooltip("era:N", title="Era"),
        ],
    )
    .properties(height=340)
)

st.altair_chart(weekday_bars, use_container_width=True)

# ──────────────────────────────────────────────────────────────────────
# Footer
# ──────────────────────────────────────────────────────────────────────

st.markdown("---")
st.caption(
    "Data source: Google Takeout search history  ·  "
    "Pipeline: Bruin  ·  Warehouse: BigQuery  ·  Viz: Streamlit + Altair"
)
