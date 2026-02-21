from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(
    page_title="The State of Stack Overflow",
    layout="wide",
)

# ──────────────────────────────────────────────────────────────────────
# Connection
# ──────────────────────────────────────────────────────────────────────

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


def run_query(filename: str) -> pd.DataFrame:
    sql = (base_path / filename).read_text()
    return get_client().query(sql).to_dataframe()


# ──────────────────────────────────────────────────────────────────────
# Load & prep
# ──────────────────────────────────────────────────────────────────────

monthly = run_raw(
    "SELECT * FROM `bruin-playground-arsalan.staging.stackoverflow_monthly` ORDER BY month"
)
tags = run_raw(
    "SELECT * FROM `bruin-playground-arsalan.staging.stackoverflow_tag_trends` ORDER BY month"
)

monthly["month"] = pd.to_datetime(monthly["month"])
tags["month"] = pd.to_datetime(tags["month"])

HIGHLIGHT = "#D55E00"
DEFAULT = "#56B4E9"
SECONDARY = "#E69F00"
MUTED = "#999999"

ERA_DOMAIN = ["Growth (2008-2014)", "Plateau (2015-2022)", "Post-ChatGPT (2023+)"]
ERA_RANGE = [DEFAULT, SECONDARY, HIGHLIGHT]

# ──────────────────────────────────────────────────────────────────────
# Header
# ──────────────────────────────────────────────────────────────────────

st.title("The State of Stack Overflow")
st.caption(
    "Monthly activity data from 2008 to present  ·  "
    "Built with Bruin + BigQuery + Streamlit"
)

latest_month = monthly["month"].max().strftime("%b %Y")
st.info(
    f"Data current through **{latest_month}**. "
    "Sources: BigQuery Public Datasets (2008-Sep 2022) + Stack Exchange API (Oct 2022-present)."
)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════
# 1. Big Number Cards
# ══════════════════════════════════════════════════════════════════════

peak_row = monthly.loc[monthly["question_count"].idxmax()]
latest_row = monthly.iloc[-1]

peak_questions = int(peak_row["question_count"])
latest_questions = int(latest_row["question_count"])
overall_decline = round(
    (latest_questions - peak_questions) / peak_questions * 100, 1
)

pre_chatgpt_monthly_avg = monthly.loc[
    ~monthly["is_post_chatgpt"], "question_count"
].mean()
post_chatgpt_monthly_avg = monthly.loc[
    monthly["is_post_chatgpt"], "question_count"
].mean()
era_decline = round(
    (post_chatgpt_monthly_avg - pre_chatgpt_monthly_avg) / pre_chatgpt_monthly_avg * 100, 1
)

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric(
        "Peak Month",
        f"{peak_questions:,}",
        peak_row["month"].strftime("%b %Y"),
        delta_color="off",
    )
with col2:
    st.metric(
        "Latest Month",
        f"{latest_questions:,}",
        latest_row["month"].strftime("%b %Y"),
        delta_color="off",
    )
with col3:
    st.metric("Decline from Peak", f"{overall_decline:+.1f}%")
with col4:
    st.metric(
        "Post-ChatGPT Avg",
        f"{post_chatgpt_monthly_avg:,.0f}/mo",
        f"{era_decline:+.1f}% vs pre-ChatGPT",
    )

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════
# 2. The Cliff — Monthly Questions
# ══════════════════════════════════════════════════════════════════════

st.subheader("Monthly Questions Asked")
st.caption(
    "Total questions posted to Stack Overflow each month since 2008. "
    "The vertical line marks November 2022 (ChatGPT public launch)."
)

cliff_bars = (
    alt.Chart(monthly)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    .encode(
        x=alt.X("month:T", title="Month"),
        y=alt.Y("question_count:Q", title="Questions"),
        color=alt.Color(
            "era:N",
            title="Era",
            scale=alt.Scale(domain=ERA_DOMAIN, range=ERA_RANGE),
        ),
        tooltip=[
            alt.Tooltip("month:T", title="Month", format="%b %Y"),
            alt.Tooltip("question_count:Q", title="Questions", format=","),
            alt.Tooltip("era:N", title="Era"),
        ],
    )
    .properties(height=380)
)

chatgpt_rule = (
    alt.Chart(pd.DataFrame({"x": [pd.Timestamp("2022-11-01")]}))
    .mark_rule(color="#333333", strokeDash=[6, 3], strokeWidth=2)
    .encode(x="x:T")
)
chatgpt_label = (
    alt.Chart(
        pd.DataFrame({
            "x": [pd.Timestamp("2022-11-01")],
            "label": ["ChatGPT launches"],
        })
    )
    .mark_text(
        align="right", dx=-8, dy=-10,
        fontSize=11, color="#333333", fontWeight="bold",
    )
    .encode(x="x:T", text="label:N")
)

pre_chatgpt_avg = monthly.loc[
    ~monthly["is_post_chatgpt"], "question_count"
].mean()
avg_rule = (
    alt.Chart(pd.DataFrame({"avg": [pre_chatgpt_avg]}))
    .mark_rule(color=MUTED, strokeDash=[6, 3], strokeWidth=1.5)
    .encode(y="avg:Q")
)
avg_text = (
    alt.Chart(
        pd.DataFrame({
            "avg": [pre_chatgpt_avg],
            "label": [f"Pre-ChatGPT avg: {pre_chatgpt_avg:,.0f}"],
        })
    )
    .mark_text(align="left", dx=5, dy=-8, color=MUTED, fontSize=11)
    .encode(y="avg:Q", text="label:N")
)

st.altair_chart(
    cliff_bars + chatgpt_rule + chatgpt_label + avg_rule + avg_text,
    use_container_width=True,
)

post_chatgpt = monthly[monthly["is_post_chatgpt"]]
if len(post_chatgpt):
    first_post = post_chatgpt.iloc[0]
    decline_since_chatgpt = round(
        (latest_questions - int(first_post["question_count"]))
        / int(first_post["question_count"])
        * 100,
        1,
    )
    st.markdown(
        f"> In **{first_post['month'].strftime('%b %Y')}**, Stack Overflow saw "
        f"**{int(first_post['question_count']):,}** questions. "
        f"By **{latest_row['month'].strftime('%b %Y')}**, that number was "
        f"**{latest_questions:,}** — a **{decline_since_chatgpt:+.1f}%** change."
    )

# ══════════════════════════════════════════════════════════════════════
# 3. Which Communities Collapsed First?
# ══════════════════════════════════════════════════════════════════════

st.markdown("---")
st.subheader("Which Communities Collapsed First?")
st.caption(
    "Each tag's monthly question volume, normalized to its all-time peak (100%). "
    "Smoothed to quarterly averages. Click a tag in the legend to highlight it. "
    "Tag-level data available through Sep 2022 (BigQuery public dataset)."
)

top_8 = (
    tags.groupby("tag")["question_count"]
    .sum()
    .nlargest(8)
    .index.tolist()
)
tags_top8 = tags[tags["tag"].isin(top_8)].copy()

tags_top8["quarter_start"] = (
    tags_top8["month"].dt.to_period("Q").dt.to_timestamp()
)
tags_quarterly = (
    tags_top8.groupby(["quarter_start", "tag"])
    .agg(
        question_count=("question_count", "mean"),
        peak_count=("peak_count", "first"),
    )
    .reset_index()
)
tags_quarterly["pct_of_peak"] = (
    tags_quarterly["question_count"] / tags_quarterly["peak_count"] * 100
).round(1)

selection = alt.selection_point(fields=["tag"], bind="legend")

tag_palette = [
    "#0072B2", "#D55E00", "#009E73", "#CC79A7",
    "#56B4E9", "#E69F00", "#F0E442", "#999999",
]

tag_lines = (
    alt.Chart(tags_quarterly)
    .mark_line(strokeWidth=2)
    .encode(
        x=alt.X("quarter_start:T", title="Quarter"),
        y=alt.Y("pct_of_peak:Q", title="% of Peak"),
        color=alt.Color(
            "tag:N",
            title="Tag",
            scale=alt.Scale(domain=top_8, range=tag_palette),
        ),
        opacity=alt.condition(selection, alt.value(1), alt.value(0.15)),
        tooltip=[
            alt.Tooltip("quarter_start:T", title="Quarter", format="%b %Y"),
            alt.Tooltip("tag:N", title="Tag"),
            alt.Tooltip("pct_of_peak:Q", title="% of Peak", format=".1f"),
            alt.Tooltip("question_count:Q", title="Avg Monthly Qs", format=",.0f"),
        ],
    )
    .properties(height=380)
    .add_params(selection)
)

chatgpt_rule_2 = (
    alt.Chart(pd.DataFrame({"x": [pd.Timestamp("2022-11-01")]}))
    .mark_rule(color="#333333", strokeDash=[6, 3], strokeWidth=1.5)
    .encode(x="x:T")
)

st.altair_chart(tag_lines + chatgpt_rule_2, use_container_width=True)

latest_tag_month = tags_top8["month"].max()
latest_tags = (
    tags_top8[tags_top8["month"] == latest_tag_month]
    .sort_values("pct_of_peak")
)
if len(latest_tags):
    most_collapsed = latest_tags.iloc[0]
    least_collapsed = latest_tags.iloc[-1]
    st.markdown(
        f"> As of **{latest_tag_month.strftime('%b %Y')}**: "
        f"**{most_collapsed['tag']}** has fallen to "
        f"**{most_collapsed['pct_of_peak']:.1f}%** of its peak, while "
        f"**{least_collapsed['tag']}** retains "
        f"**{least_collapsed['pct_of_peak']:.1f}%**."
    )

# ══════════════════════════════════════════════════════════════════════
# 4. The Answer Desert
# ══════════════════════════════════════════════════════════════════════

st.markdown("---")
st.subheader("The Answer Desert")
st.caption(
    "Are the remaining questions still getting answered? "
    "Answer rate = % of questions with at least one answer. "
    "Smoothed to quarterly averages. Detailed answer metrics available through Sep 2022."
)

monthly_with_rates = monthly[monthly["answer_rate_pct"].notna()].copy()
monthly_with_rates["quarter_start"] = (
    monthly_with_rates["month"].dt.to_period("Q").dt.to_timestamp()
)
quarterly_rates = (
    monthly_with_rates.groupby("quarter_start")
    .agg(
        answer_rate_pct=("answer_rate_pct", "mean"),
        avg_answer_count=("avg_answer_count", "mean"),
        is_post_chatgpt=("is_post_chatgpt", "last"),
    )
    .reset_index()
)

rate_col, apq_col = st.columns(2)

with rate_col:
    st.markdown("**Answer Rate**")

    answer_line = (
        alt.Chart(quarterly_rates)
        .mark_line(strokeWidth=2.5, color=DEFAULT)
        .encode(
            x=alt.X("quarter_start:T", title="Quarter"),
            y=alt.Y(
                "answer_rate_pct:Q",
                title="% Answered",
                scale=alt.Scale(zero=False),
            ),
            tooltip=[
                alt.Tooltip("quarter_start:T", title="Quarter", format="%b %Y"),
                alt.Tooltip("answer_rate_pct:Q", title="Answer Rate %", format=".1f"),
            ],
        )
        .properties(height=340)
    )
    answer_dots = (
        alt.Chart(quarterly_rates)
        .mark_circle(size=40)
        .encode(
            x="quarter_start:T",
            y="answer_rate_pct:Q",
            color=alt.condition(
                alt.datum.is_post_chatgpt,
                alt.value(HIGHLIGHT),
                alt.value(DEFAULT),
            ),
            tooltip=[
                alt.Tooltip("quarter_start:T", title="Quarter", format="%b %Y"),
                alt.Tooltip("answer_rate_pct:Q", title="Answer Rate %", format=".1f"),
            ],
        )
    )
    st.altair_chart(
        answer_line + answer_dots + chatgpt_rule_2,
        use_container_width=True,
    )

with apq_col:
    st.markdown("**Answers per Question**")

    apq_line = (
        alt.Chart(quarterly_rates)
        .mark_line(strokeWidth=2.5, color=DEFAULT)
        .encode(
            x=alt.X("quarter_start:T", title="Quarter"),
            y=alt.Y(
                "avg_answer_count:Q",
                title="Avg Answers per Question",
                scale=alt.Scale(zero=False),
            ),
            tooltip=[
                alt.Tooltip("quarter_start:T", title="Quarter", format="%b %Y"),
                alt.Tooltip("avg_answer_count:Q", title="Answers/Question", format=".2f"),
            ],
        )
        .properties(height=340)
    )
    apq_dots = (
        alt.Chart(quarterly_rates)
        .mark_circle(size=40)
        .encode(
            x="quarter_start:T",
            y="avg_answer_count:Q",
            color=alt.condition(
                alt.datum.is_post_chatgpt,
                alt.value(HIGHLIGHT),
                alt.value(DEFAULT),
            ),
            tooltip=[
                alt.Tooltip("quarter_start:T", title="Quarter", format="%b %Y"),
                alt.Tooltip("avg_answer_count:Q", title="Answers/Question", format=".2f"),
            ],
        )
    )
    st.altair_chart(
        apq_line + apq_dots + chatgpt_rule_2,
        use_container_width=True,
    )

early_rate = monthly_with_rates.loc[
    monthly_with_rates["month"] < "2015-01-01", "answer_rate_pct"
].mean()
late_rate = monthly_with_rates.loc[
    monthly_with_rates["month"] >= "2019-01-01", "answer_rate_pct"
].mean()
early_apq = monthly_with_rates.loc[
    monthly_with_rates["month"] < "2015-01-01", "avg_answer_count"
].mean()
late_apq = monthly_with_rates.loc[
    monthly_with_rates["month"] >= "2019-01-01", "avg_answer_count"
].mean()

st.markdown(
    f"> In the early years (2008-2014), **{early_rate:.1f}%** of questions received "
    f"an answer with an average of **{early_apq:.2f}** answers each. "
    f"By 2019-2022, the answer rate dropped to **{late_rate:.1f}%** "
    f"with **{late_apq:.2f}** answers per question."
)

# ══════════════════════════════════════════════════════════════════════
# 5. The Acceleration
# ══════════════════════════════════════════════════════════════════════

st.markdown("---")
st.subheader("The Acceleration")
st.caption(
    "Year-over-year change in total questions asked. "
    "Only complete calendar years are shown."
)

monthly["quarter_start"] = monthly["month"].dt.to_period("Q").dt.to_timestamp()
months_per_year = monthly.groupby("year").size()
full_years = months_per_year[months_per_year == 12].index.tolist()

annual = (
    monthly[monthly["year"].isin(full_years)]
    .groupby("year")
    .agg(total_questions=("question_count", "sum"))
    .reset_index()
)
annual["prev_year_total"] = annual["total_questions"].shift(1)
annual["yoy_change_pct"] = (
    (annual["total_questions"] - annual["prev_year_total"])
    / annual["prev_year_total"]
    * 100
).round(1)
annual["is_post_chatgpt"] = annual["year"] >= 2023
annual = annual[annual["yoy_change_pct"].notna()]

accel_chart = (
    alt.Chart(annual)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    .encode(
        x=alt.X("year:O", title="Year"),
        y=alt.Y("yoy_change_pct:Q", title="Year-over-Year Change (%)"),
        color=alt.condition(
            alt.datum.is_post_chatgpt,
            alt.value(HIGHLIGHT),
            alt.value(DEFAULT),
        ),
        tooltip=[
            alt.Tooltip("year:O", title="Year"),
            alt.Tooltip("yoy_change_pct:Q", title="YoY Change %", format=".1f"),
            alt.Tooltip("total_questions:Q", title="Total Questions", format=","),
        ],
    )
    .properties(height=340)
)

zero_rule = (
    alt.Chart(pd.DataFrame({"y": [0]}))
    .mark_rule(color="#333333", strokeWidth=1)
    .encode(y="y:Q")
)

st.altair_chart(accel_chart + zero_rule, use_container_width=True)

worst_year = annual.loc[annual["yoy_change_pct"].idxmin()]
st.markdown(
    f"> The steepest annual decline: **{int(worst_year['year'])}** at "
    f"**{worst_year['yoy_change_pct']:+.1f}%** year-over-year "
    f"({int(worst_year['total_questions']):,} total questions)."
)

# ──────────────────────────────────────────────────────────────────────
# Footer
# ──────────────────────────────────────────────────────────────────────

st.markdown("---")
st.caption(
    "Data: Google BigQuery Public Datasets + Stack Exchange API — Stack Overflow (CC BY-SA 4.0)  ·  "
    "Pipeline: Bruin  ·  Database: BigQuery  ·  Visualization: Streamlit + Altair"
)
