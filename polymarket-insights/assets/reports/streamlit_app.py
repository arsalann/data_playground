from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(
    page_title="What the Crowd Believes: Polymarket 2026",
    layout="wide",
)

PROJECT_ID = "bruin-playground-arsalan"
base_path = Path(__file__).parent

# Wong (2011) colorblind-friendly palette — Nature Methods
VERMILLION = "#D55E00"
SKY_BLUE = "#56B4E9"
ORANGE = "#E69F00"
BLUISH_GREEN = "#009E73"
REDDISH_PURPLE = "#CC79A7"
BLUE = "#0072B2"
YELLOW = "#F0E442"
GREY = "#999999"

TOPIC_COLORS = {
    "Iran Crisis": VERMILLION,
    "US Politics": BLUE,
    "Economy": BLUISH_GREEN,
    "Crypto": ORANGE,
    "Latin America": REDDISH_PURPLE,
    "Geopolitics": SKY_BLUE,
    "Other": GREY,
}


@st.cache_resource
def get_client():
    credentials = service_account.Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)


def run_raw(sql: str) -> pd.DataFrame:
    return get_client().query(sql).to_dataframe()


def build_cliff_chart(data, annotations, color, height=380):
    """Build a probability cliff line chart with annotations and a 50% reference."""
    data = data.copy()
    data["date"] = pd.to_datetime(data["date"])

    line = (
        alt.Chart(data)
        .mark_line(strokeWidth=2.5, color=color)
        .encode(
            x=alt.X("date:T", title="Date", axis=alt.Axis(format="%b %d", labelAngle=-45)),
            y=alt.Y(
                "probability_pct:Q",
                title="Implied Probability (%)",
                scale=alt.Scale(domain=[0, 105]),
                axis=alt.Axis(values=[0, 10, 25, 50, 75, 100]),
            ),
            tooltip=[
                alt.Tooltip("date:T", title="Date", format="%b %d, %Y"),
                alt.Tooltip("probability_pct:Q", title="Probability (%)", format=".1f"),
            ],
        )
    )

    area = (
        alt.Chart(data)
        .mark_area(opacity=0.08, color=color)
        .encode(
            x="date:T",
            y=alt.Y("probability_pct:Q", scale=alt.Scale(domain=[0, 105])),
        )
    )

    fifty_rule = (
        alt.Chart(pd.DataFrame({"y": [50]}))
        .mark_rule(strokeDash=[4, 4], color=GREY, strokeWidth=1)
        .encode(y="y:Q")
    )
    fifty_label = (
        alt.Chart(pd.DataFrame({"y": [50], "text": ["50%"]}))
        .mark_text(align="left", dx=4, dy=-8, fontSize=11, color=GREY)
        .encode(y="y:Q", text="text:N", x=alt.value(0))
    )

    ann_df = pd.DataFrame(annotations)
    ann_df["date"] = pd.to_datetime(ann_df["date"])

    points = (
        alt.Chart(ann_df)
        .mark_point(shape="diamond", size=80, filled=True, color=color, stroke="white", strokeWidth=1)
        .encode(x="date:T", y="probability_pct:Q")
    )

    layers = fifty_rule + fifty_label + area + line + points

    # Add text annotations with per-row positioning
    for _, row in ann_df.iterrows():
        dy = -14 if row["probability_pct"] < 50 else 14
        single = pd.DataFrame([row])
        single["date"] = pd.to_datetime(single["date"])
        t = (
            alt.Chart(single)
            .mark_text(
                align="right", dx=-10, dy=dy,
                fontSize=12, fontWeight="bold", color=color,
            )
            .encode(x="date:T", y="probability_pct:Q", text="note:N")
        )
        layers = layers + t

    return layers.properties(height=height)


# ──────────────────────────────────────────────────────────────────────
# Load all data in parallel
# ──────────────────────────────────────────────────────────────────────

khamenei_data = run_raw("""
    WITH m AS (
        SELECT question FROM `bruin-playground-arsalan.staging.polymarket_markets_enriched`
        WHERE LOWER(question) LIKE '%khamenei out%' AND LOWER(question) LIKE '%february 28%'
        ORDER BY volume_total DESC LIMIT 1
    )
    SELECT DATE(ph.timestamp) as date, ROUND(ph.price * 100, 1) as probability_pct
    FROM `bruin-playground-arsalan.raw.polymarket_price_history` ph
    JOIN m ON ph.question = m.question
    WHERE ph.outcome_label = 'Yes' AND ph.timestamp >= '2026-01-15'
    ORDER BY ph.timestamp
""")

strikes_data = run_raw("""
    WITH m AS (
        SELECT question FROM `bruin-playground-arsalan.staging.polymarket_markets_enriched`
        WHERE LOWER(question) LIKE '%us strikes iran%' AND LOWER(question) LIKE '%february 28%'
        ORDER BY volume_total DESC LIMIT 1
    )
    SELECT DATE(ph.timestamp) as date, ROUND(ph.price * 100, 1) as probability_pct
    FROM `bruin-playground-arsalan.raw.polymarket_price_history` ph
    JOIN m ON ph.question = m.question
    WHERE ph.outcome_label = 'Yes' AND ph.timestamp >= '2026-01-15'
    ORDER BY ph.timestamp
""")

shutdown_data = run_raw("""
    SELECT DATE(timestamp) as date, ROUND(price * 100, 1) as probability_pct
    FROM `bruin-playground-arsalan.raw.polymarket_price_history`
    WHERE question = 'US government shutdown Saturday?'
      AND outcome_label = 'Yes'
      AND timestamp >= '2026-01-01'
    ORDER BY timestamp
""")

fed_hold_data = run_raw("""
    SELECT DATE(timestamp) as date, ROUND(price * 100, 1) as probability_pct
    FROM `bruin-playground-arsalan.raw.polymarket_price_history`
    WHERE question = 'No change in Fed interest rates after January 2026 meeting?'
      AND outcome_label = 'Yes'
      AND timestamp >= '2025-12-01'
    ORDER BY timestamp
""")

fed_cut_data = run_raw("""
    SELECT DATE(timestamp) as date, ROUND(price * 100, 1) as probability_pct
    FROM `bruin-playground-arsalan.raw.polymarket_price_history`
    WHERE question = 'Will the Fed decrease interest rates by 25 bps after the March 2026 meeting?'
      AND outcome_label = 'Yes'
      AND timestamp >= '2025-12-01'
    ORDER BY timestamp
""")

# ──────────────────────────────────────────────────────────────────────
# Header
# ──────────────────────────────────────────────────────────────────────

st.title("What the Crowd Believes")
st.markdown(
    "Prediction markets let people bet real money on future events. "
    "When the crowd is confident, it moves the price toward 0% or 100%. "
    "When the crowd is **wrong**, the chart tells the story."
)

st.markdown("---")

# ──────────────────────────────────────────────────────────────────────
# STORY 1: The Iran Escalation
# ──────────────────────────────────────────────────────────────────────

st.header("The Iran Escalation: 1% to Certain in Three Days")
st.markdown(
    "**$131M** was wagered on whether Khamenei would be ousted as Supreme Leader "
    "by February 28, 2026. On February 25 — three days before the deadline — "
    "the market priced it at just **1.1%**. Then it happened."
)

if not khamenei_data.empty and not strikes_data.empty:
    khamenei_data["label"] = "Khamenei Ousted ($131M)"
    strikes_data["label"] = "US Strikes Iran ($90M)"
    iran_combined = pd.concat([khamenei_data, strikes_data], ignore_index=True)
    iran_combined["date"] = pd.to_datetime(iran_combined["date"])

    lines = (
        alt.Chart(iran_combined)
        .mark_line(strokeWidth=2.5)
        .encode(
            x=alt.X("date:T", title="Date", axis=alt.Axis(format="%b %d", labelAngle=-45)),
            y=alt.Y(
                "probability_pct:Q",
                title="Implied Probability (%)",
                scale=alt.Scale(domain=[0, 105]),
                axis=alt.Axis(values=[0, 10, 25, 50, 75, 100]),
            ),
            color=alt.Color(
                "label:N",
                scale=alt.Scale(
                    domain=["Khamenei Ousted ($131M)", "US Strikes Iran ($90M)"],
                    range=[VERMILLION, BLUE],
                ),
                title="Market",
            ),
            strokeDash=alt.StrokeDash(
                "label:N",
                scale=alt.Scale(
                    domain=["Khamenei Ousted ($131M)", "US Strikes Iran ($90M)"],
                    range=[[0], [6, 4]],
                ),
                title="Market",
            ),
            tooltip=[
                alt.Tooltip("label:N", title="Market"),
                alt.Tooltip("date:T", title="Date", format="%b %d, %Y"),
                alt.Tooltip("probability_pct:Q", title="Probability (%)", format=".1f"),
            ],
        )
    )

    fifty_rule = (
        alt.Chart(pd.DataFrame({"y": [50]}))
        .mark_rule(strokeDash=[4, 4], color=GREY, strokeWidth=1)
        .encode(y="y:Q")
    )
    fifty_label = (
        alt.Chart(pd.DataFrame({"y": [50], "text": ["50%"]}))
        .mark_text(align="left", dx=4, dy=-8, fontSize=11, color=GREY)
        .encode(y="y:Q", text="text:N", x=alt.value(0))
    )

    ann = pd.DataFrame([
        {"date": "2026-02-25", "probability_pct": 1.1, "note": "Feb 25: 1.1%"},
        {"date": "2026-03-01", "probability_pct": 99.9, "note": "Resolved YES"},
    ])
    ann["date"] = pd.to_datetime(ann["date"])

    ann_points = (
        alt.Chart(ann)
        .mark_point(shape="diamond", size=80, filled=True, color=VERMILLION, stroke="white", strokeWidth=1)
        .encode(x="date:T", y="probability_pct:Q")
    )
    ann_low = (
        alt.Chart(ann[ann["probability_pct"] < 50])
        .mark_text(align="right", dx=-10, dy=-14, fontSize=12, fontWeight="bold", color=VERMILLION)
        .encode(x="date:T", y="probability_pct:Q", text="note:N")
    )
    ann_high = (
        alt.Chart(ann[ann["probability_pct"] >= 50])
        .mark_text(align="right", dx=-10, dy=14, fontSize=12, fontWeight="bold", color=VERMILLION)
        .encode(x="date:T", y="probability_pct:Q", text="note:N")
    )

    chart = (
        (fifty_rule + fifty_label + lines + ann_points + ann_low + ann_high)
        .properties(height=380)
        .configure_legend(
            orient="top", direction="horizontal",
            titleFontSize=12, labelFontSize=12, symbolStrokeWidth=2.5,
        )
    )
    st.altair_chart(chart, use_container_width=True)

st.markdown("---")

# ──────────────────────────────────────────────────────────────────────
# STORY 2: The Government Shutdown — Deal Collapses
# ──────────────────────────────────────────────────────────────────────

st.header("The Deal That Fell Apart: Government Shutdown")
st.markdown(
    "On January 23, with a deal seemingly in hand, the market priced a government shutdown "
    "at just **6.5%**. The next day, the deal collapsed. Eight days later, the government shut down. "
    "**$157M** was wagered — the highest-volume resolved event of Q1 2026."
)

if not shutdown_data.empty:
    chart = build_cliff_chart(
        shutdown_data,
        annotations=[
            {"date": "2026-01-23", "probability_pct": 6.5, "note": "Jan 23: 6.5% — deal looks done"},
            {"date": "2026-01-24", "probability_pct": 37.5, "note": "Jan 24: deal collapses"},
            {"date": "2026-01-31", "probability_pct": 99.7, "note": "Jan 31: shutdown confirmed"},
        ],
        color=BLUE,
    )
    st.altair_chart(chart, use_container_width=True)

st.markdown("---")

# ──────────────────────────────────────────────────────────────────────
# STORY 3: The Fed Trapped — Rate Cuts Evaporate
# ──────────────────────────────────────────────────────────────────────

st.header("The Fed Trapped: Rate Cuts Evaporated")
st.markdown(
    "In December 2025 the market gave a rate cut at the January meeting **50/50 odds**. "
    "Then inflation data came in, the Iran crisis spiked oil prices, and the Fed froze. "
    "By March the probability of a rate cut collapsed to **0.3%** — the market concluded "
    "the Fed is trapped between inflation and recession."
)

if not fed_hold_data.empty and not fed_cut_data.empty:
    fed_hold_data["label"] = "Fed Holds Rates — Jan ($107M)"
    fed_cut_data["label"] = "Fed Cuts 25bp — Mar"
    fed_combined = pd.concat([fed_hold_data, fed_cut_data], ignore_index=True)
    fed_combined["date"] = pd.to_datetime(fed_combined["date"])

    # Only show 2026 data for cleaner chart
    fed_combined = fed_combined[fed_combined["date"] >= "2025-12-01"]

    lines = (
        alt.Chart(fed_combined)
        .mark_line(strokeWidth=2.5)
        .encode(
            x=alt.X("date:T", title="Date", axis=alt.Axis(format="%b %d", labelAngle=-45)),
            y=alt.Y(
                "probability_pct:Q",
                title="Implied Probability (%)",
                scale=alt.Scale(domain=[0, 105]),
                axis=alt.Axis(values=[0, 10, 25, 50, 75, 100]),
            ),
            color=alt.Color(
                "label:N",
                scale=alt.Scale(
                    domain=["Fed Holds Rates — Jan ($107M)", "Fed Cuts 25bp — Mar"],
                    range=[BLUISH_GREEN, ORANGE],
                ),
                title="Market",
            ),
            strokeDash=alt.StrokeDash(
                "label:N",
                scale=alt.Scale(
                    domain=["Fed Holds Rates — Jan ($107M)", "Fed Cuts 25bp — Mar"],
                    range=[[0], [6, 4]],
                ),
                title="Market",
            ),
            tooltip=[
                alt.Tooltip("label:N", title="Market"),
                alt.Tooltip("date:T", title="Date", format="%b %d, %Y"),
                alt.Tooltip("probability_pct:Q", title="Probability (%)", format=".1f"),
            ],
        )
    )

    fifty_rule = (
        alt.Chart(pd.DataFrame({"y": [50]}))
        .mark_rule(strokeDash=[4, 4], color=GREY, strokeWidth=1)
        .encode(y="y:Q")
    )
    fifty_label = (
        alt.Chart(pd.DataFrame({"y": [50], "text": ["50%"]}))
        .mark_text(align="left", dx=4, dy=-8, fontSize=11, color=GREY)
        .encode(y="y:Q", text="text:N", x=alt.value(0))
    )

    # Annotation: the crossover moment
    cross_ann = pd.DataFrame([
        {"date": "2026-01-10", "probability_pct": 96.0, "note": "Jan 10: Fed hold hits 96%"},
        {"date": "2026-03-18", "probability_pct": 0.3, "note": "Mar 18: cut hope dies at 0.3%"},
    ])
    cross_ann["date"] = pd.to_datetime(cross_ann["date"])

    ann_high = (
        alt.Chart(cross_ann[cross_ann["probability_pct"] >= 50])
        .mark_text(align="right", dx=-10, dy=14, fontSize=12, fontWeight="bold", color=BLUISH_GREEN)
        .encode(x="date:T", y="probability_pct:Q", text="note:N")
    )
    ann_low = (
        alt.Chart(cross_ann[cross_ann["probability_pct"] < 50])
        .mark_text(align="left", dx=10, dy=-14, fontSize=12, fontWeight="bold", color=ORANGE)
        .encode(x="date:T", y="probability_pct:Q", text="note:N")
    )
    ann_pts = (
        alt.Chart(cross_ann)
        .mark_point(shape="diamond", size=80, filled=True, stroke="white", strokeWidth=1)
        .encode(
            x="date:T", y="probability_pct:Q",
            color=alt.Color(
                "probability_pct:Q",
                scale=alt.Scale(domain=[0, 100], range=[ORANGE, BLUISH_GREEN]),
                legend=None,
            ),
        )
    )

    chart = (
        (fifty_rule + fifty_label + lines + ann_pts + ann_high + ann_low)
        .properties(height=380)
        .configure_legend(
            orient="top", direction="horizontal",
            titleFontSize=12, labelFontSize=12, symbolStrokeWidth=2.5,
        )
    )
    st.altair_chart(chart, use_container_width=True)

st.markdown("---")

# ──────────────────────────────────────────────────────────────────────
# CHART 4: Q1 2026 Timeline — Confirmed by the Crowd
# ──────────────────────────────────────────────────────────────────────

st.header("Q1 2026: Confirmed by the Crowd")
st.markdown(
    "Every major non-sports event that resolved **YES** on Polymarket in Q1 2026, "
    "sized by how much money was on the line."
)

timeline = run_raw("""
    SELECT
        question,
        ROUND(volume_total / 1e6, 1) as volume_m,
        DATE(end_date) as resolution_date,
        CASE
            WHEN LOWER(question) LIKE '%iran%' OR LOWER(question) LIKE '%khamenei%'
                 OR LOWER(question) LIKE '%fordow%' OR LOWER(question) LIKE '%hormuz%' THEN 'Iran Crisis'
            WHEN LOWER(question) LIKE '%trump%' OR LOWER(question) LIKE '%government shutdown%'
                 OR LOWER(question) LIKE '%impeach%' OR LOWER(question) LIKE '%fed %'
                 OR LOWER(question) LIKE '%warsh%' OR LOWER(question) LIKE '%resign%' THEN 'US Politics'
            WHEN LOWER(question) LIKE '%recession%' OR LOWER(question) LIKE '%tariff%'
                 OR LOWER(question) LIKE '%oil%' OR LOWER(question) LIKE '%crude%'
                 OR LOWER(question) LIKE '%s&p%' THEN 'Economy'
            WHEN LOWER(question) LIKE '%bitcoin%' OR LOWER(question) LIKE '%ethereum%'
                 OR LOWER(question) LIKE '%crypto%' OR LOWER(question) LIKE '%satoshi%' THEN 'Crypto'
            WHEN LOWER(question) LIKE '%venezuela%' OR LOWER(question) LIKE '%maduro%'
                 OR LOWER(question) LIKE '%mexico%' OR LOWER(question) LIKE '%cartel%' THEN 'Latin America'
            WHEN LOWER(question) LIKE '%russia%' OR LOWER(question) LIKE '%ukraine%'
                 OR LOWER(question) LIKE '%china%' OR LOWER(question) LIKE '%xi %'
                 OR LOWER(question) LIKE '%greenland%' OR LOWER(question) LIKE '%nato%' THEN 'Geopolitics'
            ELSE 'Other'
        END AS theme
    FROM `bruin-playground-arsalan.staging.polymarket_markets_enriched`
    WHERE end_date >= '2026-01-01' AND end_date < '2026-04-02'
      AND is_resolved_yes = true
      AND volume_total > 5000000
      AND LOWER(question) NOT LIKE '%super bowl%'
      AND LOWER(question) NOT LIKE '%nba%'
      AND LOWER(question) NOT LIKE '%nfl%'
      AND LOWER(question) NOT LIKE '%premier%'
      AND LOWER(question) NOT LIKE '%la liga%'
      AND LOWER(question) NOT LIKE '%champions%'
      AND LOWER(question) NOT LIKE '%ufc%'
      AND LOWER(question) NOT LIKE '%fight%'
      AND LOWER(question) NOT LIKE '% vs.%'
      AND LOWER(question) NOT LIKE '% vs %'
      AND LOWER(question) NOT LIKE '%win on%'
      AND LOWER(question) NOT LIKE '%spread%'
      AND LOWER(question) NOT LIKE '%over/under%'
      AND LOWER(question) NOT LIKE '%moneyline%'
      AND LOWER(question) NOT LIKE '%ncaa%'
      AND LOWER(question) NOT LIKE '%boxing%'
      AND LOWER(question) NOT LIKE '%stranger things%'
      AND LOWER(question) NOT LIKE '%oscar%'
      AND LOWER(question) NOT LIKE '%grammy%'
      AND LOWER(question) NOT LIKE '%tiktok%'
    ORDER BY end_date ASC, volume_total DESC
""")

if not timeline.empty:
    timeline["resolution_date"] = pd.to_datetime(timeline["resolution_date"])
    timeline["short_q"] = timeline["question"].str[:75]

    timeline = timeline.sort_values("volume_m", ascending=False).drop_duplicates(
        subset=["short_q"], keep="first"
    ).head(25)

    timeline["label"] = (
        timeline["resolution_date"].dt.strftime("%b %d")
        + " — "
        + timeline["short_q"]
    )

    domain_themes = list(TOPIC_COLORS.keys())
    range_colors = list(TOPIC_COLORS.values())

    bars = (
        alt.Chart(timeline)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("volume_m:Q", title="Trading Volume ($M)"),
            y=alt.Y(
                "label:N",
                sort=alt.EncodingSortField(field="resolution_date", order="ascending"),
                title=None,
                axis=alt.Axis(labelLimit=500),
            ),
            color=alt.Color(
                "theme:N",
                scale=alt.Scale(domain=domain_themes, range=range_colors),
                title="Theme",
                legend=alt.Legend(orient="top", direction="horizontal", columns=4),
            ),
            tooltip=[
                alt.Tooltip("question:N", title="Question"),
                alt.Tooltip("resolution_date:T", title="Resolved", format="%b %d, %Y"),
                alt.Tooltip("volume_m:Q", title="Volume ($M)", format=",.1f"),
                alt.Tooltip("theme:N", title="Theme"),
            ],
        )
    )

    text_labels = (
        alt.Chart(timeline)
        .mark_text(align="left", dx=4, fontSize=11, color="#333333")
        .encode(
            x=alt.X("volume_m:Q"),
            y=alt.Y(
                "label:N",
                sort=alt.EncodingSortField(field="resolution_date", order="ascending"),
            ),
            text=alt.Text("volume_m:Q", format="$,.0fM"),
        )
    )

    chart = (bars + text_labels).properties(
        height=max(len(timeline) * 30, 380)
    )

    st.altair_chart(chart, use_container_width=True)

    total_vol = timeline["volume_m"].sum()
    st.markdown(
        f"**${total_vol:,.0f}M** in total volume across {len(timeline)} confirmed events."
    )

st.markdown("---")
st.caption(
    "Data: Polymarket Gamma API & CLOB API. Top 10,000 markets by all-time volume. "
    "Prices represent implied probabilities backed by real money. "
    "Colors follow the Wong (2011) colorblind-friendly palette."
)
