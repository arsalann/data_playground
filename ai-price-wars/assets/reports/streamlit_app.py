import altair as alt
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(page_title="The AI Price Wars", layout="wide")

PROJECT_ID = "bruin-playground-arsalan"

# Wong (2011) colorblind-friendly palette
VERMILLION = "#D55E00"
SKY_BLUE = "#56B4E9"
ORANGE = "#E69F00"
BLUISH_GREEN = "#009E73"
REDDISH_PURPLE = "#CC79A7"
BLUE = "#0072B2"
YELLOW = "#F0E442"
GREY = "#999999"


@st.cache_resource
def get_client():
    credentials = service_account.Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)


def run_query(sql: str) -> pd.DataFrame:
    return get_client().query(sql).to_dataframe()


# ── Load all data up front ──────────────────────────────────────────

models = run_query("""
    SELECT
        model_id, model_name, provider, provider_tier,
        price_blended_per_mtok, price_input_per_mtok, price_output_per_mtok,
        is_free, model_created_at, arena_text_elo, arena_text_rank,
        context_length, model_family, is_reasoning_model, price_per_elo_point
    FROM staging.models_enriched
""")

# ── Derived datasets ────────────────────────────────────────────────

paid = models[models["price_blended_per_mtok"] > 0].copy()
elo_models = models[models["arena_text_elo"].notna() & (models["price_blended_per_mtok"] > 0)].copy()
elo_models["arena_text_elo"] = elo_models["arena_text_elo"].astype(int)
elo_models["short_name"] = elo_models["model_name"].str.replace(r"^[^:]+:\s*", "", regex=True)

# Pareto frontier: models where no other model is both cheaper AND higher ELO
pareto = []
for _, row in elo_models.sort_values("arena_text_elo", ascending=False).iterrows():
    if not pareto or row["price_blended_per_mtok"] < pareto[-1]["price_blended_per_mtok"]:
        pareto.append(row.to_dict())
pareto_df = pd.DataFrame(pareto)
elo_models["is_pareto"] = elo_models["model_id"].isin(pareto_df["model_id"])

# ── Header ──────────────────────────────────────────────────────────

st.title("The AI Price Wars")
st.caption(
    "348 models, 56 providers, 18 quality-ranked models  ·  "
    "Data: OpenRouter + LMArena + Polymarket  ·  Pipeline: Bruin + BigQuery  ·  April 2026"
)
st.markdown("---")

# ════════════════════════════════════════════════════════════════════
# CHART 1: The Efficiency Frontier — price vs quality scatter
# Shows: you pay 18x more for only 5% better quality
# ════════════════════════════════════════════════════════════════════

st.subheader("You Pay 18x More for 5% Better Quality")
st.caption(
    "Each dot is one of the 18 models with both pricing and a human-preference "
    "quality score (Arena ELO). The orange line connects the Pareto frontier — "
    "models where no other model is both cheaper and better."
)

provider_sel = alt.selection_point(fields=["provider"], bind="legend")

provider_domain = sorted(elo_models["provider"].unique().tolist())
provider_colors = {
    "Anthropic": VERMILLION, "Google": BLUISH_GREEN, "OpenAI": SKY_BLUE,
    "Qwen": ORANGE, "Z-Ai": REDDISH_PURPLE, "Moonshotai": BLUE, "Xiaomi": YELLOW,
}
provider_range = [provider_colors.get(p, GREY) for p in provider_domain]

points = (
    alt.Chart(elo_models)
    .mark_circle(size=120, stroke="white", strokeWidth=1)
    .encode(
        x=alt.X(
            "price_blended_per_mtok:Q",
            title="Blended price ($/MTok) — log scale",
            scale=alt.Scale(type="log"),
        ),
        y=alt.Y(
            "arena_text_elo:Q",
            title="Arena text ELO (higher = better)",
            scale=alt.Scale(domain=[1425, 1510]),
        ),
        color=alt.Color(
            "provider:N",
            title="Provider",
            scale=alt.Scale(domain=provider_domain, range=provider_range),
            legend=alt.Legend(orient="top", columns=7),
        ),
        opacity=alt.condition(provider_sel, alt.value(0.9), alt.value(0.15)),
        tooltip=[
            alt.Tooltip("short_name:N", title="Model"),
            alt.Tooltip("provider:N", title="Provider"),
            alt.Tooltip("arena_text_elo:Q", title="ELO"),
            alt.Tooltip("price_blended_per_mtok:Q", title="Price $/MTok", format="$.3f"),
        ],
    )
    .properties(height=450)
    .add_params(provider_sel)
)

# Pareto frontier line
frontier_line = (
    alt.Chart(pareto_df.sort_values("arena_text_elo"))
    .mark_line(color=ORANGE, strokeWidth=2, strokeDash=[6, 3])
    .encode(
        x="price_blended_per_mtok:Q",
        y="arena_text_elo:Q",
    )
)

# Labels on Pareto models
pareto_labels = (
    alt.Chart(pareto_df)
    .mark_text(align="left", dx=8, dy=-2, fontSize=11, fontWeight="bold", color="#333")
    .encode(
        x="price_blended_per_mtok:Q",
        y="arena_text_elo:Q",
        text="short_name:N",
    )
)

st.altair_chart(points + frontier_line + pareto_labels, use_container_width=True)

# Quantify the insight
best = elo_models.loc[elo_models["arena_text_elo"].idxmax()]
cheapest_pareto = pareto_df.loc[pareto_df["price_blended_per_mtok"].idxmin()]
elo_gap_pct = round((best["arena_text_elo"] - cheapest_pareto["arena_text_elo"]) / best["arena_text_elo"] * 100, 1)
price_ratio = round(best["price_blended_per_mtok"] / cheapest_pareto["price_blended_per_mtok"], 0)

best_price = f"{best['price_blended_per_mtok']:.2f}"
cheap_price = f"{cheapest_pareto['price_blended_per_mtok']:.2f}"
corr_val = f"{elo_models['price_blended_per_mtok'].corr(elo_models['arena_text_elo']):.2f}"

st.text(
    f"The best model is {best['short_name']} (ELO {int(best['arena_text_elo'])}) "
    f"at ${best_price}/MTok. The cheapest model on the Pareto frontier is "
    f"{cheapest_pareto['short_name']} (ELO {int(cheapest_pareto['arena_text_elo'])}) "
    f"at ${cheap_price}/MTok — {elo_gap_pct}% less quality for {int(price_ratio)}x less money. "
    f"The correlation between price and ELO across all 18 models is r = {corr_val}, "
    f"meaning price is a poor predictor of quality."
)

st.markdown("---")

# ════════════════════════════════════════════════════════════════════
# Methodology and limitations
# ════════════════════════════════════════════════════════════════════

st.subheader("Data sources, methodology, and limitations")

st.markdown("""
**Sources:**

| Source | What | Coverage | Accessed |
|--------|------|----------|----------|
| [OpenRouter API](https://openrouter.ai/api/v1/models) | Model catalog + real-time pricing | 348 models, 56 providers | April 2026 |
| [LMArena](https://lmarena.ai) via [wulong.dev](https://api.wulong.dev) | Human-preference ELO rankings | 50 text-ranked models, 18 matched | March 2026 |
| [Polymarket](https://polymarket.com) | Prediction market odds + volume | 11 provider matches | Q1 2026 |

**How the numbers were calculated:**

- **Blended price** = 75% input price + 25% output price (per million tokens), reflecting a typical chat workload where prompts are longer than responses.
- **Arena ELO** = [LMArena](https://lmarena.ai) human-preference ranking where users compare two model responses head-to-head. Higher = better. Only 18 of 348 OpenRouter models could be matched to Arena rankings — matching uses normalized model names (stripping version suffixes, dots, hyphens).
- **Pareto frontier** = the set of models where no other model is simultaneously cheaper AND has a higher ELO. These are the "rational" choices.
- **Quarterly medians** use the date the model first appeared on OpenRouter (`model_created_at`), not when the model was originally released. Quarters with fewer than 2 launches per tier are excluded.
- **Reasoning models** are identified by name patterns: `thinking`, `reasoning`, `o1`, `o3`, `o4`, `deepseek-r1`.

**What this data cannot tell you:**

- **Quality beyond the top 18.** Only 5% of models have Arena ELO rankings. We literally cannot say whether cheaper models are "worse" for the other 330 models.
- **Real-world performance.** ELO is based on human preference in a chat setting. It doesn't measure coding ability, instruction following, or domain-specific performance.
- **Actual price history.** The "deflation" story uses launch dates + current prices, not historical price tracking. We don't know if models were cheaper or more expensive at launch.
- **Usage-weighted pricing.** A model at $0.05/MTok that nobody uses is not the same as one at $10/MTok serving millions of requests. We have no usage data.
- **OpenRouter ≠ the whole market.** Especially for Chinese models, domestic platforms may have different pricing.

Built with [Bruin](https://getbruin.com), BigQuery, Streamlit, and Altair.
""")
