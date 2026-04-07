# AI Price Wars

Analysis of the AI model marketplace revealing the "intelligence deflation" phenomenon — how the cost of frontier-level AI has collapsed faster than any technology in history. Combines real-time pricing data from OpenRouter (350+ models), historical pricing from llm-prices.com, human-preference quality rankings from LMArena, and cross-references with Polymarket prediction markets on AI competitive outcomes.

## What This Pipeline Does

The AI industry is engaged in the most aggressive price war in tech history. This pipeline captures that war in data:
- **348 models** from **56 providers** with real-time pricing
- **105 historical pricing records** tracking cost changes over time
- **266 quality rankings** across 7 categories from the Arena leaderboard
- **Cross-market analysis** linking $124M in Polymarket AI betting volume with actual model performance

## Key Findings

### 1. The 98% Collapse: Intelligence Deflation Is Real
The median price to launch a new AI model dropped from **$37.50/MTok in Q2 2023** to **$0.80/MTok in Q1 2026** — a **98% decline** in under 3 years. The cheapest frontier model (Gemini 2.0 Flash Lite at $0.075/MTok) costs just **0.25% of GPT-4's launch price**.

### 2. The Quality Gap Is Closing Fast
Open source models (Qwen, Mistral, DeepSeek) now achieve **ELO 1449** — within **4% of the #1 model** (Claude Opus 4.6 at ELO 1504). But open source models cost **$0.88/MTok** vs proprietary at **$6.01/MTok** — a **7x price premium** for a 4% quality edge.

### 3. Google Wins the Value War
Google's Gemini 3.1 Flash Lite Preview offers the **best price-per-ELO-point** ($0.39) among all Arena-ranked models — a 17x better value than Anthropic's Claude Opus 4.6 ($6.65). Google's median model price ($0.56/MTok) is **6x cheaper** than OpenAI's ($3.44/MTok).

### 4. The Prediction Market Knew
Polymarket bettors wagered **$24.6M** on OpenAI outcomes, **$16.9M** on Google, and **$13.4M** on Anthropic. The crowd correctly predicted Anthropic would have the "best AI model" in Feb 2026 (resolved YES at $0.999) — and the Arena ELO data confirms Claude Opus 4.6 holds the #1 spot.

### 5. Chinese Labs Are the Dark Horses
Z.ai (GLM 5, ELO 1456), MoonshotAI (Kimi K2.5, ELO 1453), and Xiaomi (MiMo-V2-Pro, ELO 1444) all rank in the **top 40** globally while charging **$0.73-$1.50/MTok** — dramatically undercutting Western frontier labs. Qwen alone has **49 models** on OpenRouter, more than Anthropic (15) and Google (30) combined.

### 6. The $262.50 Outlier
OpenAI's reasoning models (o1, o3) cost up to **$262.50/MTok** — 875x the cheapest frontier model. This "ultra-premium" tier exists only for OpenAI, suggesting they're the only lab that can command this pricing for specialized reasoning capabilities.

## Data Sources

| Source | URL | Auth | What It Provides |
|--------|-----|------|-----------------|
| **OpenRouter** | `openrouter.ai/api/v1/models` | None | 348 models, real-time pricing, context lengths, metadata |
| **llm-prices.com** | `llm-prices.com/historical-v1.json` | None | 105 pricing records with historical date ranges |
| **LMArena** (via wulong.dev) | `api.wulong.dev/arena-ai-leaderboards/v1/` | None | 266 ELO rankings across 7 categories |
| **Polymarket** (cross-pipeline) | Existing `raw.polymarket_markets` table | N/A | AI-related prediction market data for cross-enrichment |

## Assets

### Raw Layer

| Asset | Type | Strategy | Description |
|-------|------|----------|-------------|
| `raw.openrouter_models` | Python | append | Fetches 348+ model catalog with pricing from OpenRouter API |
| `raw.llm_price_history` | Python | append | Fetches historical pricing data from llm-prices.com |
| `raw.arena_leaderboard` | Python | append | Fetches ELO rankings across 7 categories from Arena API |

### Staging Layer

| Asset | Type | Strategy | Depends On | Description |
|-------|------|----------|------------|-------------|
| `staging.models_enriched` | SQL | create+replace | `raw.openrouter_models`, `raw.arena_leaderboard` | Unified model catalog with normalized providers, price tiers, model families, and Arena ELO join |
| `staging.price_evolution` | SQL | create+replace | `raw.llm_price_history` | Price change timeline with period-over-period analysis and cumulative reductions |
| `staging.intelligence_deflation` | SQL | create+replace | `raw.llm_price_history` | Capability-tier pricing analysis benchmarked against GPT-4 launch price |
| `staging.provider_comparison` | SQL | create+replace | `staging.models_enriched` | Provider-level aggregation of pricing, quality, and value metrics |
| `staging.ai_market_crossover` | SQL | create+replace | `staging.models_enriched`, `staging.provider_comparison` | Cross-references AI pricing with Polymarket prediction market data |

## Run Commands

```bash
# Validate the pipeline
bruin validate ai-price-wars/

# Run raw assets individually
bruin run ai-price-wars/assets/raw/openrouter_models.py
bruin run ai-price-wars/assets/raw/llm_price_history.py
bruin run ai-price-wars/assets/raw/arena_leaderboard.py

# Run staging assets (order matters for dependencies)
bruin run ai-price-wars/assets/staging/models_enriched.sql
bruin run ai-price-wars/assets/staging/price_evolution.sql
bruin run ai-price-wars/assets/staging/intelligence_deflation.sql
bruin run ai-price-wars/assets/staging/provider_comparison.sql
bruin run ai-price-wars/assets/staging/ai_market_crossover.sql

# Run full pipeline end-to-end
bruin run ai-price-wars/

# Run bruin ai enhance on any asset
bruin ai enhance ai-price-wars/assets/raw/openrouter_models.py
```

## Known Limitations

- **llm-prices.com** has limited historical depth — most records are current pricing snapshots with only 1 model showing a historical price change. The "deflation" narrative is best constructed from the OpenRouter model catalog with `model_created_at` dates.
- **Arena leaderboard** historical snapshots are limited to recent dates (~March 2026+). No deep time-series of ELO rankings available.
- **Arena-to-OpenRouter matching** uses normalized string matching which may miss some models with very different naming conventions between the two platforms.
- **OpenRouter pricing** includes some sentinel values (e.g., -1M for router/meta models) that should be filtered in analysis.
- **Polymarket cross-enrichment** depends on the existing `raw.polymarket_markets` table from the polymarket-insights pipeline being populated.
