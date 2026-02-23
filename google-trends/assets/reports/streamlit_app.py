from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(
    page_title="The AI Effect on Search",
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


# ──────────────────────────────────────────────────────────────────────
# Load & prep
# ──────────────────────────────────────────────────────────────────────

indexed = run_raw(
    "SELECT * FROM `bruin-playground-arsalan.staging.trends_ai_effect_weekly` ORDER BY term, month"
)
terms = run_raw(
    "SELECT * FROM `bruin-playground-arsalan.staging.trends_ai_effect_terms` ORDER BY pct_change"
)

indexed["month"] = pd.to_datetime(indexed["month"])
CHATGPT_DATE = pd.Timestamp("2022-11-30")

HIGHLIGHT = "#D55E00"
DEFAULT = "#56B4E9"
SECONDARY = "#E69F00"
MUTED = "#999999"
ACCENT = "#009E73"

# ──────────────────────────────────────────────────────────────────────
# Header
# ──────────────────────────────────────────────────────────────────────

st.title("The AI Effect on Search")
st.caption(
    "How ChatGPT reshaped what America Googles  ·  "
    "Built with Bruin + BigQuery + Streamlit"
)

st.markdown(
    "> **Hypothesis:** Since ChatGPT launched (Nov 30, 2022), people increasingly "
    "ask LLM agents the questions they used to Google — price checks, weather, "
    "stock lookups, how-to queries. Meanwhile, Google remains the go-to for "
    "discovering entities, events, and brands that LLMs can't replace."
)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════
# 1. The Rise of "AI" — Hockey Stick
# ══════════════════════════════════════════════════════════════════════

st.subheader('The Rise of "AI"')
st.caption(
    'Monthly average search score for the term "ai" per US media market. '
    "All scores are relative within each market (0–100 scale). "
    "The vertical line marks ChatGPT's public launch."
)

ai_data = indexed[indexed["term"] == "ai"].copy()

ai_area = (
    alt.Chart(ai_data)
    .mark_area(
        opacity=0.6,
        line={"strokeWidth": 2.5, "color": DEFAULT},
        color=alt.Gradient(
            gradient="linear",
            stops=[
                alt.GradientStop(color=DEFAULT, offset=0),
                alt.GradientStop(color="#56B4E900", offset=1),
            ],
            x1=1, x2=1, y1=1, y2=0,
        ),
    )
    .encode(
        x=alt.X("month:T", title=""),
        y=alt.Y("avg_score:Q", title="Avg Score per Market"),
        tooltip=[
            alt.Tooltip("month:T", title="Month", format="%b %Y"),
            alt.Tooltip("avg_score:Q", title="Avg Score", format=".1f"),
            alt.Tooltip("indexed_score:Q", title="Index (baseline=100)", format=".0f"),
        ],
    )
    .properties(height=340)
)

chatgpt_rule = (
    alt.Chart(pd.DataFrame({"x": [CHATGPT_DATE]}))
    .mark_rule(color="#333333", strokeDash=[6, 3], strokeWidth=2)
    .encode(x="x:T")
)
chatgpt_label = (
    alt.Chart(pd.DataFrame({"x": [CHATGPT_DATE], "label": ["ChatGPT launches"]}))
    .mark_text(
        align="left", dx=8, dy=10,
        fontSize=12, color="#333333", fontWeight="bold",
    )
    .encode(x="x:T", text="label:N")
)

st.altair_chart(ai_area + chatgpt_rule + chatgpt_label, use_container_width=True)

pre_ai = ai_data[ai_data["month"] < CHATGPT_DATE]["avg_score"].mean()
post_ai = ai_data[ai_data["month"] >= CHATGPT_DATE]["avg_score"].mean()
latest_ai = ai_data.iloc[-1]["avg_score"]
st.markdown(
    f"> Before ChatGPT, \"ai\" averaged **{pre_ai:.1f}** per market. "
    f"After: **{post_ai:.1f}** — a **{post_ai / pre_ai:.0f}x** increase. "
    f"By late 2025 it reached **{latest_ai:.0f}**, making it one of the most "
    f"consistently searched terms in the country."
)

# ══════════════════════════════════════════════════════════════════════
# 2. Lookup Terms Are Fading
# ══════════════════════════════════════════════════════════════════════

st.markdown("---")
st.subheader("Lookup Terms Are Fading")
st.caption(
    "Indexed search intensity for factual lookup queries (prices, weather, stocks) "
    "that LLMs can answer. All terms indexed to 100 at baseline (Jul–Nov 2022, "
    "just before ChatGPT). Values below 100 = decline from baseline. "
    "Click a term in the legend to isolate it."
)

lookups = indexed[indexed["query_intent"] == "lookup"].copy()
lookup_terms = lookups.groupby("term")["avg_score"].mean().sort_values(ascending=False).head(8).index.tolist()
lookups = lookups[lookups["term"].isin(lookup_terms)]

selection = alt.selection_point(fields=["term"], bind="legend")

lookup_palette = [
    "#D55E00", "#E69F00", "#CC79A7", "#0072B2",
    "#009E73", "#56B4E9", "#F0E442", "#999999",
]

lookup_lines = (
    alt.Chart(lookups)
    .mark_line(strokeWidth=2)
    .encode(
        x=alt.X("month:T", title=""),
        y=alt.Y("indexed_score:Q", title="Index (baseline = 100)"),
        color=alt.Color(
            "term:N",
            title="Term",
            scale=alt.Scale(domain=lookup_terms, range=lookup_palette),
        ),
        opacity=alt.condition(selection, alt.value(1), alt.value(0.15)),
        tooltip=[
            alt.Tooltip("month:T", title="Month", format="%b %Y"),
            alt.Tooltip("term:N", title="Term"),
            alt.Tooltip("indexed_score:Q", title="Index", format=".0f"),
            alt.Tooltip("avg_score:Q", title="Avg Score", format=".1f"),
        ],
    )
    .properties(height=380)
    .add_params(selection)
)

baseline_rule = (
    alt.Chart(pd.DataFrame({"y": [100]}))
    .mark_rule(color=MUTED, strokeDash=[6, 3], strokeWidth=1.5)
    .encode(y="y:Q")
)
baseline_label = (
    alt.Chart(pd.DataFrame({"y": [100], "label": ["Baseline (100)"]}))
    .mark_text(align="left", dx=5, dy=-8, color=MUTED, fontSize=11)
    .encode(y="y:Q", text="label:N")
)

st.altair_chart(
    lookup_lines + chatgpt_rule + chatgpt_label + baseline_rule + baseline_label,
    use_container_width=True,
)

latest_lookups = lookups.groupby("term")["indexed_score"].last()
below_baseline = (latest_lookups < 100).sum()
st.markdown(
    f"> By late 2025, **{below_baseline}** of {len(latest_lookups)} tracked lookup terms "
    f"sit below their pre-ChatGPT baseline. Ethereum price is the steepest faller, "
    f"but weather and stock lookups also show persistent declines."
)

# ══════════════════════════════════════════════════════════════════════
# 3. Entity Terms Are Thriving
# ══════════════════════════════════════════════════════════════════════

st.markdown("---")
st.subheader("Entity Terms Are Thriving")
st.caption(
    "Indexed search intensity for entity/discovery queries — brands, sports, "
    "entertainment, people. These are searches that LLMs cannot replace because "
    "they require real-time discovery. Same baseline indexing as above."
)

entities = indexed[indexed["query_intent"] == "entity"].copy()
entity_terms = entities.groupby("term")["avg_score"].mean().sort_values(ascending=False).head(8).index.tolist()
entities = entities[entities["term"].isin(entity_terms)]

selection2 = alt.selection_point(fields=["term"], bind="legend")

entity_palette = [
    "#56B4E9", "#0072B2", "#009E73", "#CC79A7",
    "#E69F00", "#D55E00", "#F0E442", "#999999",
]

entity_lines = (
    alt.Chart(entities)
    .mark_line(strokeWidth=2)
    .encode(
        x=alt.X("month:T", title=""),
        y=alt.Y("indexed_score:Q", title="Index (baseline = 100)"),
        color=alt.Color(
            "term:N",
            title="Term",
            scale=alt.Scale(domain=entity_terms, range=entity_palette),
        ),
        opacity=alt.condition(selection2, alt.value(1), alt.value(0.15)),
        tooltip=[
            alt.Tooltip("month:T", title="Month", format="%b %Y"),
            alt.Tooltip("term:N", title="Term"),
            alt.Tooltip("indexed_score:Q", title="Index", format=".0f"),
            alt.Tooltip("avg_score:Q", title="Avg Score", format=".1f"),
        ],
    )
    .properties(height=380)
    .add_params(selection2)
)

st.altair_chart(
    entity_lines + chatgpt_rule + chatgpt_label + baseline_rule + baseline_label,
    use_container_width=True,
)

latest_entities = entities.groupby("term")["indexed_score"].last()
above_baseline = (latest_entities >= 100).sum()
st.markdown(
    f"> **{above_baseline}** of {len(latest_entities)} tracked entity terms sit above their "
    f"pre-ChatGPT baseline. Google is becoming less of an answer engine and more of a "
    f"discovery engine — people still need it to find out *what's happening*, "
    f"not *how things work*."
)

# ══════════════════════════════════════════════════════════════════════
# 4. The Scoreboard — Before vs After
# ══════════════════════════════════════════════════════════════════════

st.markdown("---")
st.subheader("The Scoreboard")
st.caption(
    "Percentage change in average per-market search score after ChatGPT's launch "
    "versus before, for all consistently trending terms (active 5+ months in each era). "
    "The 20 biggest decliners and growers, colored by whether an LLM could answer the query."
)

filtered_terms = terms[
    (terms["pre_months"] >= 8) & (terms["post_months"] >= 12)
].copy()

losers = filtered_terms.head(20).copy()
winners = filtered_terms.tail(20).copy()

losers_col, winners_col = st.columns(2)

with losers_col:
    st.markdown("**Biggest Decliners**")
    loser_chart = (
        alt.Chart(losers)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("pct_change:Q", title="% Change"),
            y=alt.Y("term:N", title="", sort=alt.EncodingSortField("pct_change", order="ascending")),
            color=alt.condition(
                alt.datum.is_llm_answerable,
                alt.value(HIGHLIGHT),
                alt.value(MUTED),
            ),
            tooltip=[
                alt.Tooltip("term:N", title="Term"),
                alt.Tooltip("query_category:N", title="Category"),
                alt.Tooltip("pct_change:Q", title="% Change", format="+.1f"),
                alt.Tooltip("pre_avg_score:Q", title="Pre Avg", format=".1f"),
                alt.Tooltip("post_avg_score:Q", title="Post Avg", format=".1f"),
            ],
        )
        .properties(height=450)
    )
    st.altair_chart(loser_chart, use_container_width=True)

with winners_col:
    st.markdown("**Biggest Growers**")
    winner_chart = (
        alt.Chart(winners)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("pct_change:Q", title="% Change"),
            y=alt.Y("term:N", title="", sort=alt.EncodingSortField("pct_change", order="descending")),
            color=alt.condition(
                alt.datum.is_llm_answerable,
                alt.value(HIGHLIGHT),
                alt.value(DEFAULT),
            ),
            tooltip=[
                alt.Tooltip("term:N", title="Term"),
                alt.Tooltip("query_category:N", title="Category"),
                alt.Tooltip("pct_change:Q", title="% Change", format="+.1f"),
                alt.Tooltip("pre_avg_score:Q", title="Pre Avg", format=".1f"),
                alt.Tooltip("post_avg_score:Q", title="Post Avg", format=".1f"),
            ],
        )
        .properties(height=450)
    )
    st.altair_chart(winner_chart, use_container_width=True)

n_llm_declining = len(filtered_terms[filtered_terms["is_llm_answerable"] & (filtered_terms["pct_change"] < 0)])
n_llm_total = len(filtered_terms[filtered_terms["is_llm_answerable"]])
n_entity_growing = len(filtered_terms[~filtered_terms["is_llm_answerable"] & (filtered_terms["pct_change"] > 0)])
n_entity_total = len(filtered_terms[~filtered_terms["is_llm_answerable"]])
st.markdown(
    f"> Orange = LLM-answerable queries. Among them, **{n_llm_declining}/{n_llm_total}** "
    f"({n_llm_declining/max(n_llm_total,1)*100:.0f}%) declined post-ChatGPT. "
    f"Among entity queries, **{n_entity_growing}/{n_entity_total}** "
    f"({n_entity_growing/max(n_entity_total,1)*100:.0f}%) grew."
)

# ══════════════════════════════════════════════════════════════════════
# 5. The Full Distribution
# ══════════════════════════════════════════════════════════════════════

st.markdown("---")
st.subheader("The Full Distribution")
st.caption(
    "Distribution of post-ChatGPT change (%) for all consistently trending terms, "
    "split by query type. The vertical line marks zero change. "
    "LLM-answerable queries (orange) skew left; entity queries (blue) skew right."
)

hist_data = filtered_terms.copy()
hist_data["type_label"] = hist_data["is_llm_answerable"].map({
    True: "LLM-Answerable",
    False: "Entity / Event",
})

histogram = (
    alt.Chart(hist_data)
    .mark_bar(opacity=0.6, cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
    .encode(
        x=alt.X("pct_change:Q", bin=alt.Bin(step=10), title="% Change Post-ChatGPT"),
        y=alt.Y("count():Q", title="Number of Terms", stack=None),
        color=alt.Color(
            "type_label:N",
            title="Query Type",
            scale=alt.Scale(
                domain=["LLM-Answerable", "Entity / Event"],
                range=[HIGHLIGHT, DEFAULT],
            ),
        ),
        tooltip=[
            alt.Tooltip("type_label:N", title="Type"),
            alt.Tooltip("count():Q", title="Terms"),
        ],
    )
    .properties(height=340)
)

zero_rule = (
    alt.Chart(pd.DataFrame({"x": [0]}))
    .mark_rule(color="#333333", strokeWidth=1.5)
    .encode(x="x:Q")
)

st.altair_chart(histogram + zero_rule, use_container_width=True)

median_llm = filtered_terms[filtered_terms["is_llm_answerable"]]["pct_change"].median()
median_entity = filtered_terms[~filtered_terms["is_llm_answerable"]]["pct_change"].median()
st.markdown(
    f"> Median change for LLM-answerable terms: **{median_llm:+.1f}%**. "
    f"Median change for entity terms: **{median_entity:+.1f}%**. "
    f"The gap of **{median_entity - median_llm:.1f}pp** suggests a real, "
    f"if modest, shift in what people use Google for."
)

# ──────────────────────────────────────────────────────────────────────
# Footer
# ──────────────────────────────────────────────────────────────────────

st.markdown("---")
st.caption(
    "Data: Google Trends via BigQuery Public Datasets (Apr 2021 – Nov 2025, trimmed)  ·  "
    "Pipeline: Bruin  ·  Database: BigQuery  ·  Visualization: Streamlit + Altair"
)
st.caption(
    "Caveats: Google Trends tracks the top trending terms per market, not total query volume. "
    "Lookup declines may partly reflect other factors (e.g. crypto winter for ETH/BTC). "
    "Scores are relative within each market. This analysis is directional, not definitive."
)
