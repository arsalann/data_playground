/* @bruin

name: staging.provider_comparison
type: bq.sql
description: |
  Strategic provider-level aggregation for AI market competitive intelligence, analyzing
  56 AI companies across pricing tiers, model portfolios, quality benchmarks, and value
  propositions. Essential for understanding competitive dynamics in the "intelligence
  deflation" phenomenon where frontier AI capabilities become commodity-priced.

  This table reveals which providers dominate specific market segments: Frontier labs
  (OpenAI, Anthropic, Google) lead on quality but command premium pricing; Major Open
  Source providers (Meta, Mistral) offer competitive performance at lower costs;
  Cloud Providers (AWS, Azure, GCP) provide enterprise-grade reliability; Emerging
  and Challenger providers compete on aggressive pricing and specialized capabilities.

  Key insights enabled:
  - Provider tier analysis: 5 distinct tiers from Frontier to Cloud Provider
  - Pricing strategy patterns: some providers span 5000x price range (premium to commodity)
  - Quality concentration: only 12.5% of providers (7/56) have Arena-ranked models
  - Portfolio breadth: ranges from single-model specialists to 50+ model catalogs
  - Value positioning: identifies which providers offer best price/performance ratios

  Data characteristics:
  - 56 providers with complete model portfolio aggregations
  - Price metrics in USD per million tokens (industry standard)
  - Arena ELO rankings sparse but authoritative (only top-tier models ranked)
  - Context lengths up to 2M+ tokens for long-form document processing
  - Mix of free-tier and premium pricing models across all provider types

  Business applications: vendor selection, pricing benchmarking, competitive positioning,
  market opportunity identification, AI democratization research, cost optimization.
connection: bruin-playground-arsalan
tags:
  - ai-price-wars
  - competitive-intelligence
  - provider-analysis
  - market-segmentation
  - pricing-strategy
  - intelligence-deflation
  - value-analysis
  - vendor-comparison
  - staging

materialization:
  type: table
  strategy: create+replace

depends:
  - staging.models_enriched

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: provider
    type: VARCHAR
    description: |
      Standardized AI provider company name (OpenAI, Anthropic, Google, Meta, etc.).
      Normalized from raw API data for consistent analysis. Primary business identifier
      for competitive intelligence and vendor comparison. 56 distinct providers ranging
      from frontier AI labs to emerging startups. Each provider represents a unique
      strategic position in the AI market ecosystem.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: provider_tier
    type: VARCHAR
    description: |
      Strategic market classification: Frontier (cutting-edge R&D labs), Major Open Source
      (large tech with OSS focus), Challenger (aggressive pricing/specialization),
      Emerging (startups and new entrants), Cloud Provider (enterprise infrastructure).
      Based on business model, market position, and technological capabilities.
      Critical for competitive analysis and market segmentation.
    checks:
      - name: not_null
  - name: total_models
    type: INTEGER
    description: |
      Total model portfolio size on OpenRouter marketplace. Ranges from single-model
      specialists to comprehensive catalogs (50+ models). Indicates provider's breadth
      of offerings and market coverage strategy. Average: 6.3 models per provider with
      high variance reflecting different business approaches (focused vs. platform).
    checks:
      - name: not_null
  - name: free_models
    type: INTEGER
    description: |
      Count of models offered at zero cost to users. Strategic indicator of freemium
      business models, developer ecosystem building, and market penetration tactics.
      Most providers offer 0-1 free models; higher counts suggest aggressive user
      acquisition or open-source commitment. Essential for analyzing AI democratization.
    checks:
      - name: not_null
  - name: paid_models
    type: INTEGER
    description: |
      Count of monetized models requiring payment per token usage. Core revenue-generating
      offerings that fund R&D and operations. Higher counts indicate mature product
      portfolios and diversified revenue streams. Used to assess commercial viability
      and market positioning strategy across different capability tiers.
    checks:
      - name: not_null
  - name: min_input_price
    type: DOUBLE
    description: |
      Most affordable input token pricing in USD per million tokens among paid models.
      Represents provider's entry-level commercial offering and competitive floor price.
      Range: $0.017-$3.75/MTok. Critical for cost-sensitive applications and market
      accessibility analysis. NULL for providers with only free models (2 providers).
  - name: max_input_price
    type: DOUBLE
    description: |
      Premium input token pricing in USD per million tokens for highest-tier models.
      Represents provider's flagship capabilities and maximum value extraction strategy.
      Range: $0-$150/MTok. Zero values indicate free-only providers. Extreme outliers
      ($150/MTok) signal ultra-premium positioning for specialized applications.
    checks:
      - name: not_null
  - name: median_blended_price
    type: DOUBLE
    description: |
      Middle-point blended pricing (75% input + 25% output) across paid model portfolio
      in USD per million tokens. Represents typical customer cost for chat/completion
      workloads. More stable than averages due to outlier resistance. Range: $0.04-$6/MTok.
      NULL for free-only providers. Key metric for budget planning and TCO analysis.
  - name: avg_blended_price
    type: DOUBLE
    description: |
      Mean blended pricing across paid models using industry-standard 3:1 input:output
      ratio weighting. Influenced by premium model outliers, providing insight into
      portfolio value positioning. Range: $0.04-$12.81/MTok. Higher than median indicates
      portfolio skewed toward premium offerings. Critical for revenue analysis.
  - name: max_context_length
    type: INTEGER
    description: |
      Largest context window offered by provider in tokens. Technical capability indicator
      for long-form document processing, complex reasoning, and enterprise applications.
      Average: 345K tokens with extreme variance (4K to 2M+ tokens). Frontier providers
      typically lead in context length innovation. Measured in tokens (4 chars ≈ 1 token).
    checks:
      - name: not_null
  - name: best_arena_text_elo
    type: INTEGER
    description: |
      Highest Chatbot Arena text generation ELO rating among provider's models. Gold
      standard quality benchmark from human preference evaluations. Only 7 providers
      have Arena-ranked models, indicating quality concentration among elite providers.
      Range: 1400-1500 ELO. Higher scores indicate superior chat/reasoning capabilities.
  - name: best_arena_text_rank
    type: INTEGER
    description: |
      Best leaderboard position (lowest number = highest rank) for provider's top model
      in Chatbot Arena text rankings. Competitive positioning metric where rank 1 is
      strongest performer. Only available for 7 providers with Arena presence. Used
      to identify quality leaders and track competitive positioning over time.
  - name: arena_ranked_models
    type: INTEGER
    description: |
      Count of provider's models appearing in Chatbot Arena rankings. Indicates depth
      of quality portfolio and research capability. Most providers (87.5%) have zero
      ranked models, showing quality concentration among elite labs. Higher counts
      suggest consistent high-quality model development across multiple releases.
    checks:
      - name: not_null
  - name: best_value_model
    type: VARCHAR
    description: |
      Identifier of provider's most cost-efficient model based on price per ELO point
      ratio. Strategic insight for budget-conscious customers seeking optimal price/
      performance. Only available for providers with Arena rankings (7 providers).
      Often differs from cheapest or highest-quality model, revealing value sweet spots.
  - name: best_value_price_per_elo
    type: DOUBLE
    description: |
      Lowest price per ELO point ratio among provider's Arena-ranked models, measured
      in USD per million tokens per ELO point. Quantifies cost-efficiency for quality-
      conscious applications. Range: $0.39-$4.10 per MTok per ELO. Lower values indicate
      superior value propositions. Key metric for ROI-driven vendor selection.
  - name: price_range_ratio
    type: DOUBLE
    description: |-
      Ratio of maximum to minimum input pricing within provider's paid portfolio.
      Indicates pricing strategy diversity and market coverage breadth. Range: 1x (uniform
      pricing) to 5000x (extreme tiering). High ratios suggest portfolio spanning commodity
      to premium segments. NULL for free-only providers. Critical for understanding
      provider's market positioning strategy and customer segmentation approach.

@bruin */

WITH provider_stats AS (
    SELECT
        provider,
        provider_tier,
        COUNT(*) AS total_models,
        COUNTIF(is_free) AS free_models,
        COUNTIF(NOT is_free) AS paid_models,
        MIN(CASE WHEN NOT is_free AND price_input_per_mtok > 0 THEN price_input_per_mtok END) AS min_input_price,
        MAX(price_input_per_mtok) AS max_input_price,
        APPROX_QUANTILES(CASE WHEN NOT is_free AND price_blended_per_mtok > 0 THEN price_blended_per_mtok END, 2)[OFFSET(1)] AS median_blended_price,
        AVG(CASE WHEN NOT is_free AND price_blended_per_mtok > 0 THEN price_blended_per_mtok END) AS avg_blended_price,
        MAX(context_length) AS max_context_length,
        MAX(arena_text_elo) AS best_arena_text_elo,
        MIN(CASE WHEN arena_text_rank IS NOT NULL THEN arena_text_rank END) AS best_arena_text_rank,
        COUNTIF(arena_text_elo IS NOT NULL) AS arena_ranked_models
    FROM staging.models_enriched
    GROUP BY provider, provider_tier
),

best_value AS (
    SELECT
        provider,
        model_name AS best_value_model,
        price_per_elo_point AS best_value_price_per_elo,
        ROW_NUMBER() OVER (PARTITION BY provider ORDER BY price_per_elo_point ASC) AS rn
    FROM staging.models_enriched
    WHERE price_per_elo_point IS NOT NULL AND price_per_elo_point > 0
)

SELECT
    ps.*,
    bv.best_value_model,
    ROUND(bv.best_value_price_per_elo, 6) AS best_value_price_per_elo,
    ROUND(
        CASE WHEN ps.min_input_price > 0
        THEN ps.max_input_price / ps.min_input_price
        ELSE NULL END,
    1) AS price_range_ratio
FROM provider_stats ps
LEFT JOIN best_value bv ON ps.provider = bv.provider AND bv.rn = 1
ORDER BY ps.best_arena_text_elo DESC NULLS LAST
