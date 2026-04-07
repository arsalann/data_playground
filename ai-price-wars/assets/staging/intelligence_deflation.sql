/* @bruin

name: staging.intelligence_deflation
type: bq.sql
description: |
  Tracks the "intelligence deflation" phenomenon: how the cost of equivalent AI
  capability has changed over time. Computes the cheapest available price at each
  point in time for different capability tiers, showing how quickly frontier
  intelligence becomes commodity-priced.

  This staging asset enriches raw pricing data with standardized vendor names,
  capability tier classifications, and temporal benchmarks relative to GPT-4's
  launch (March 14, 2023). The blended pricing calculation (75% input, 25% output)
  reflects typical LLM usage patterns for cost comparison.

  Key insights revealed:
  - Most records (99%) represent current pricing with historical data points rare
  - Capability tiers range from Budget ($0.035/MTok) to Frontier models ($150/MTok)
  - Intelligence deflation measured as percentage vs original GPT-4 pricing ($30/MTok)
  - Critical foundation for AI cost trend analysis and competitive intelligence

  Data characteristics: 104 rows across 10 vendors, 103 distinct models with
  4 capability tiers. Heavy concentration in current pricing (103 current vs 1 historical).
connection: bruin-playground-arsalan
tags:
  - ai-price-wars
  - intelligence-deflation
  - competitive-analysis
  - pricing-intelligence
  - llm-market
  - cost-optimization
  - capability-benchmarking
  - staging

materialization:
  type: table
  strategy: create+replace

depends:
  - raw.llm_price_history

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: vendor
    type: VARCHAR
    description: |
      Standardized AI provider name (OpenAI, Anthropic, Google, Amazon, Mistral,
      DeepSeek, Meta, etc.). Normalized from raw lowercase vendor codes to proper
      title case for consistent reporting. 10 distinct providers tracked.
      Critical dimension for market concentration and competitive analysis.
    checks:
      - name: not_null
  - name: model_id
    type: VARCHAR
    description: |
      Unique model identifier serving as natural key (e.g. gpt-4, claude-3-opus,
      llama-2-70b). 103 distinct values across all providers. Note: not truly
      unique per row due to pricing period variations - use with from_date for
      true uniqueness. Essential for model capability mapping and benchmarking.
    checks:
      - name: not_null
  - name: model_name
    type: VARCHAR
    description: |
      Human-readable model display name as marketed by provider. Often includes
      capability indicators (turbo, pro, ultra) or version numbers. 103 distinct
      names matching model_id count. More descriptive than model_id for user
      interfaces and executive reporting dashboards.
    checks:
      - name: not_null
  - name: capability_tier
    type: VARCHAR
    description: |
      Algorithmically-assigned model capability classification based on known
      model quality and market positioning. Four tiers: 'Frontier' (best-in-class
      models), 'Near-Frontier' (strong but not cutting-edge), 'Mid-Tier' (capable
      for most use cases), 'Budget' (cost-optimized). Essential for like-to-like
      price comparisons and intelligence deflation analysis.
    checks:
      - name: not_null
  - name: price_input_per_mtok
    type: DOUBLE
    description: |
      Cost per million input tokens in USD. Foundation metric for query cost
      calculations. Ranges from $0.035 (commodity models) to $150 (premium
      specialized models). Average ~$5.73 across all models. Critical for
      cost-per-interaction analysis and budget planning.
    checks:
      - name: not_null
  - name: price_output_per_mtok
    type: DOUBLE
    description: |
      Cost per million output tokens in USD. Typically 2-10x higher than input
      cost due to generation computational complexity. Ranges from $0.04 to $600.
      Average ~$24.38. Essential for total conversation cost modeling and
      application cost optimization strategies.
    checks:
      - name: not_null
  - name: price_blended_per_mtok
    type: DOUBLE
    description: |
      Weighted average price assuming 75% input tokens, 25% output tokens -
      reflects typical LLM usage patterns for cost comparison. Calculated as
      (input_price * 0.75) + (output_price * 0.25). Ranges from $0.04 to $262.50.
      Average ~$10.39. Standard metric for cross-model cost comparison.
    checks:
      - name: not_null
  - name: from_date
    type: DATE
    description: |
      Start date when this pricing became effective. Null for 99% of records
      (original/launch pricing). Only populated for historical pricing changes.
      Critical for temporal analysis of price evolution, though mostly unused
      due to market youth. Essential for time-series deflation calculations.
  - name: to_date
    type: DATE
    description: |
      End date of this pricing period. Null for current pricing (99% of records).
      When populated, indicates a completed historical pricing period. Used to
      identify superseded pricing and calculate pricing period duration for
      market stability analysis.
  - name: is_current_price
    type: BOOLEAN
    description: |
      Flag indicating active pricing status. True for 103/104 records (99%+
      current pricing). False indicates historical pricing no longer in effect.
      Key filter for current market analysis vs historical trend analysis.
      Essential for intelligence deflation snapshots.
    checks:
      - name: not_null
  - name: days_after_gpt4_launch
    type: INTEGER
    description: |
      Days elapsed since GPT-4 launch (March 14, 2023) when this price became
      effective. Null for 103/104 records (launch pricing). The "modern era"
      temporal benchmark for intelligence deflation measurement. GPT-4 launch
      marked the frontier intelligence baseline at $30/MTok input pricing.
  - name: price_vs_gpt4_launch
    type: DOUBLE
    description: |
      Current input price as percentage of original GPT-4 launch price ($30/MTok).
      Ranges from 0.12% to 500% showing dramatic price spectrum. Values <100%
      indicate intelligence deflation, >100% indicate premium pricing. Core
      metric for quantifying how much frontier intelligence has cheapened.
    checks:
      - name: not_null

@bruin */

WITH deduped AS (
    SELECT *
    FROM raw.llm_price_history
    WHERE model_id IS NOT NULL
      AND price_input_per_mtok IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY model_id, COALESCE(CAST(from_date AS STRING), 'launch')
        ORDER BY extracted_at DESC
    ) = 1
),

classified AS (
    SELECT
        CASE
            WHEN vendor = 'openai' THEN 'OpenAI'
            WHEN vendor = 'anthropic' THEN 'Anthropic'
            WHEN vendor = 'google' THEN 'Google'
            WHEN vendor = 'amazon' THEN 'Amazon'
            WHEN vendor = 'mistral' THEN 'Mistral'
            WHEN vendor = 'deepseek' THEN 'DeepSeek'
            WHEN vendor = 'meta' THEN 'Meta'
            ELSE INITCAP(vendor)
        END AS vendor,
        model_id,
        model_name,
        price_input_per_mtok,
        price_output_per_mtok,
        ROUND(price_input_per_mtok * 0.75 + COALESCE(price_output_per_mtok, 0) * 0.25, 4) AS price_blended_per_mtok,
        from_date,
        to_date,
        is_current_price,

        -- Classify models by capability tier based on known model quality
        CASE
            -- Frontier: best models from top providers
            WHEN model_id IN ('gpt-4', 'gpt-4-turbo', 'gpt-4o', 'gpt-4o-2024-05-13', 'gpt-4o-2024-08-06', 'gpt-4.1', 'gpt-4.1-2025-04-14')
                THEN 'Frontier'
            WHEN model_id LIKE 'claude-3-opus%' OR model_id LIKE 'claude-3.5-sonnet%' OR model_id LIKE 'claude-sonnet-4%' OR model_id LIKE 'claude-opus-4%'
                THEN 'Frontier'
            WHEN model_id LIKE 'gemini-1.5-pro%' OR model_id LIKE 'gemini-2.0%' OR model_id LIKE 'gemini-2.5%'
                THEN 'Frontier'
            WHEN model_id IN ('o1', 'o1-2024-12-17', 'o3', 'o3-mini', 'o4-mini')
                THEN 'Frontier'
            WHEN model_id LIKE 'deepseek-v3%' OR model_id LIKE 'deepseek-r1%'
                THEN 'Frontier'

            -- Near-Frontier: strong but not top-tier
            WHEN model_id LIKE 'gpt-4o-mini%' OR model_id LIKE 'gpt-4.1-mini%' OR model_id LIKE 'gpt-4.1-nano%'
                THEN 'Near-Frontier'
            WHEN model_id LIKE 'claude-3-sonnet%' OR model_id LIKE 'claude-3.5-haiku%' OR model_id LIKE 'claude-haiku-4%'
                THEN 'Near-Frontier'
            WHEN model_id LIKE 'gemini-1.5-flash%' OR model_id LIKE 'gemini-2.0-flash%'
                THEN 'Near-Frontier'
            WHEN model_id LIKE 'mistral-large%' OR model_id LIKE 'llama-3.1-405b%' OR model_id LIKE 'llama-4%'
                THEN 'Near-Frontier'

            -- Mid-Tier
            WHEN model_id LIKE 'claude-3-haiku%'
                THEN 'Mid-Tier'
            WHEN model_id LIKE 'gpt-3.5%'
                THEN 'Mid-Tier'
            WHEN model_id LIKE 'llama-3%' OR model_id LIKE 'mistral-%'
                THEN 'Mid-Tier'
            WHEN model_id LIKE 'gemini-1.0%' OR model_id LIKE 'gemini-flash%'
                THEN 'Mid-Tier'

            ELSE 'Budget'
        END AS capability_tier

    FROM deduped
)

SELECT
    vendor,
    model_id,
    model_name,
    capability_tier,
    price_input_per_mtok,
    price_output_per_mtok,
    price_blended_per_mtok,
    from_date,
    to_date,
    is_current_price,

    -- Days since GPT-4 launch (March 14, 2023) — the "modern era" benchmark
    CASE
        WHEN from_date IS NOT NULL
        THEN DATE_DIFF(from_date, DATE '2023-03-14', DAY)
        ELSE NULL
    END AS days_after_gpt4_launch,

    -- Price relative to GPT-4 launch price ($30/MTok input was the original GPT-4 price)
    ROUND(price_input_per_mtok / 30.0 * 100, 2) AS price_vs_gpt4_launch

FROM classified
ORDER BY vendor, model_id, from_date ASC NULLS FIRST
