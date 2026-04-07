/* @bruin

name: staging.ai_market_crossover
type: bq.sql
description: |
  Cross-market intelligence asset combining Polymarket prediction markets with AI
  model pricing reality to reveal market perception vs. actual competitive dynamics.
  This unique dataset shows where the crowd's betting behavior aligns or diverges
  from technical capabilities and pricing strategies in the AI arms race.

  Enables powerful analytical narratives: "The crowd bet $8.7M that Microsoft would
  NOT have the top AI model — and their pricing data confirms they're positioned as
  cloud-provider tier, not frontier." Or: "Meta's Llama was heavily bet against in
  early 2024, but their free models now challenge paid competitors on performance."

  Key insights:
  - Market sentiment vs. pricing reality: high-volume bets often contradict value metrics
  - Quality concentration: only ~47% of providers (7/15) have Arena ELO rankings despite market attention
  - Prediction accuracy: tracks which crowd predictions proved correct post-resolution
  - Price-performance disconnects: identifies overvalued/undervalued providers by market perception

  Data characteristics:
  - 15 providers with significant market attention (both betting markets AND model catalogs)
  - Volume range: $2.7M-$24.6M indicating serious financial interest in AI outcomes
  - 73% market resolution rate (11/15 markets have Polymarket questions)
  - Massive pricing spreads: cheapest models at $0.025/MTok, premium models up to $262.50/MTok
  - Cross-validation between human-preference rankings (Arena ELO) and market pricing

  Business applications: investment thesis validation, competitive intelligence,
  market timing analysis, bias detection in prediction markets, AI democratization research.
connection: bruin-playground-arsalan
tags:
  - cross-market-analysis
  - prediction-markets
  - ai-competitive-intelligence
  - market-sentiment
  - betting-volume
  - price-performance
  - crowd-wisdom
  - polymarket
  - ai-price-wars
  - staging

materialization:
  type: table
  strategy: create+replace

depends:
  - staging.models_enriched
  - staging.provider_comparison

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: provider
    type: VARCHAR
    description: |
      Normalized AI provider name (OpenAI, Anthropic, Google, Meta, etc.). Primary
      entity identifier for cross-market analysis. Note: Not all 15 providers have
      corresponding Polymarket questions (4 providers have NULL betting data),
      creating natural control groups for market attention vs. technical merit comparison.
    checks:
      - name: not_null
  - name: polymarket_question
    type: VARCHAR
    description: |
      Aggregated Polymarket prediction market questions about this provider's competitive
      position, concatenated with ' | ' delimiter when multiple questions exist. Questions
      focus on "best AI model" competitions, frontier model releases, and market leadership
      claims. Length: 50-194 characters. NULL for 4 providers without market attention,
      indicating market bias toward established names despite technical capabilities.
  - name: polymarket_volume
    type: DOUBLE
    description: |
      Total betting volume in USD across all markets mentioning this provider. Range:
      $2.7M-$24.6M with average $11.3M, indicating substantial financial interest in
      AI competitive outcomes. High volume suggests either controversial positioning
      or clear market leadership. NULL for providers without prediction markets.
      Volume correlates with provider brand recognition, not necessarily technical merit.
  - name: market_resolved_yes
    type: BOOLEAN
    description: |
      Whether any Polymarket question about this provider resolved to YES (provider was
      the best/first). TRUE for 9 providers (82% of markets with questions), FALSE for 2
      providers, suggesting either crowd accuracy or question bias toward likely winners.
      NULL for providers without prediction markets. Critical for validating crowd wisdom
      against actual model performance and market positioning.
  - name: market_last_price
    type: DOUBLE
    description: |
      Final trading price representing implied probability (0.0-1.0) that this provider
      would be the best/first in prediction market questions. Range: 0.001-1.0 with
      average 0.82, indicating generally optimistic crowd sentiment. Prices near 1.0
      suggest market certainty, while low prices indicate contrarian positions.
      NULL for providers without betting markets.
  - name: total_models_on_openrouter
    type: INTEGER
    description: |
      Count of models this provider has available on OpenRouter marketplace. Range:
      1-65 models with average 16.3, indicating portfolio breadth strategy. Higher
      counts suggest platform approach (comprehensive offerings) vs. focused approach
      (flagship models). No correlation with Polymarket attention - some high-volume
      providers have few models, while catalog leaders have minimal betting interest.
    checks:
      - name: not_null
  - name: best_arena_elo
    type: INTEGER
    description: |
      Highest Chatbot Arena text generation ELO rating among provider's models. Only
      7 providers (~47%) have Arena rankings, showing quality concentration among elite
      labs. Average: 1466 ELO among ranked providers. NULL values don't indicate poor
      quality - many providers focus on specialized use cases not covered by Arena's
      general chat evaluation. Critical for validating market predictions against
      human-preference benchmarks.
  - name: cheapest_blended_price
    type: DOUBLE
    description: |
      Most affordable blended pricing (75% input + 25% output tokens) among provider's
      paid models in USD per million tokens. Range: $0.025-$0.717/MTok with average
      $0.20/MTok. Represents provider's entry-level commercial offering for cost-sensitive
      applications. Low prices indicate commodity positioning or aggressive market
      penetration strategy. All providers offer some budget option.
    checks:
      - name: not_null
  - name: priciest_blended_price
    type: DOUBLE
    description: |
      Premium blended pricing for provider's highest-tier model in USD per million tokens.
      Extreme range: $0.25-$262.50/MTok with average $21.05/MTok, showing massive
      pricing strategy differences. Values >$50/MTok indicate ultra-premium positioning
      for specialized capabilities (reasoning models, long context). Ratio with cheapest
      price reveals portfolio breadth and market segmentation approach.
    checks:
      - name: not_null
  - name: provider_tier
    type: VARCHAR
    description: |
      Strategic market classification based on business model and capabilities:
      Frontier (OpenAI, Anthropic, Google - cutting-edge R&D), Major Open Source
      (Meta, Mistral - large-scale open models), Challenger (xAI, Cohere - specialized
      competitors), Cloud Provider (Amazon, Microsoft - enterprise infrastructure),
      Emerging (startups and new entrants). Five distinct tiers represented, enabling
      analysis of competitive dynamics within and across market segments.
    checks:
      - name: not_null

@bruin */

WITH ai_markets AS (
    SELECT
        question,
        COALESCE(volume_total, 0) AS volume_total,
        last_trade_price,
        closed,
        CASE
            WHEN closed = TRUE AND last_trade_price >= 0.95 THEN TRUE
            ELSE FALSE
        END AS resolved_yes,
        CASE
            WHEN LOWER(question) LIKE '%anthropic%' THEN 'Anthropic'
            WHEN LOWER(question) LIKE '%openai%' THEN 'OpenAI'
            WHEN LOWER(question) LIKE '%google%' OR LOWER(question) LIKE '%gemini%' THEN 'Google'
            WHEN LOWER(question) LIKE '%microsoft%' THEN 'Microsoft'
            WHEN LOWER(question) LIKE '%meta %' OR LOWER(question) LIKE '%llama%' THEN 'Meta'
            WHEN LOWER(question) LIKE '%mistral%' THEN 'Mistral'
            WHEN LOWER(question) LIKE '%xai%' OR LOWER(question) LIKE '%grok%' THEN 'xAI'
            WHEN LOWER(question) LIKE '%deepseek%' THEN 'DeepSeek'
            WHEN LOWER(question) LIKE '%tencent%' THEN 'Tencent'
            WHEN LOWER(question) LIKE '%baidu%' THEN 'Baidu'
            WHEN LOWER(question) LIKE '%meituan%' THEN 'Meituan'
            ELSE NULL
        END AS mapped_provider
    FROM raw.polymarket_markets
    WHERE (
        LOWER(question) LIKE '%best ai model%'
        OR LOWER(question) LIKE '%top ai model%'
        OR LOWER(question) LIKE '%ai model%released%'
        OR LOWER(question) LIKE '%frontier model%'
    )
    AND LOWER(question) NOT LIKE '%thai%'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY extracted_at DESC) = 1
),

ai_market_agg AS (
    SELECT
        mapped_provider,
        STRING_AGG(question, ' | ' ORDER BY volume_total DESC LIMIT 3) AS polymarket_question,
        SUM(volume_total) AS polymarket_volume,
        LOGICAL_OR(resolved_yes) AS market_resolved_yes,
        MAX(CASE WHEN resolved_yes THEN last_trade_price ELSE last_trade_price END) AS market_last_price
    FROM ai_markets
    WHERE mapped_provider IS NOT NULL
    GROUP BY mapped_provider
),

provider_data AS (
    SELECT
        provider,
        provider_tier,
        COUNT(*) AS total_models_on_openrouter,
        MAX(arena_text_elo) AS best_arena_elo,
        ROUND(MIN(CASE WHEN price_blended_per_mtok > 0 THEN price_blended_per_mtok END), 4) AS cheapest_blended_price,
        ROUND(MAX(price_blended_per_mtok), 2) AS priciest_blended_price
    FROM staging.models_enriched
    GROUP BY provider, provider_tier
)

SELECT
    COALESCE(pd.provider, am.mapped_provider) AS provider,
    am.polymarket_question,
    ROUND(am.polymarket_volume, 0) AS polymarket_volume,
    am.market_resolved_yes,
    am.market_last_price,
    pd.total_models_on_openrouter,
    pd.best_arena_elo,
    pd.cheapest_blended_price,
    pd.priciest_blended_price,
    pd.provider_tier
FROM provider_data pd
FULL OUTER JOIN ai_market_agg am ON pd.provider = am.mapped_provider
WHERE am.polymarket_volume IS NOT NULL OR pd.best_arena_elo IS NOT NULL
ORDER BY COALESCE(am.polymarket_volume, 0) DESC
