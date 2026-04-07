/* @bruin

name: staging.price_evolution
type: bq.sql
description: |
  Transforms raw LLM pricing history into a comprehensive price evolution timeline for analyzing
  the "intelligence deflation" phenomenon. This asset enriches each pricing period with temporal
  analysis, period-over-period changes, and cumulative cost reduction metrics since launch.

  Core functionality:
  - Deduplicates overlapping pricing records from the source API
  - Standardizes vendor names across providers (OpenAI, Anthropic, Google, etc.)
  - Calculates blended pricing using industry-standard 75/25 input/output token weighting
  - Computes period-over-period price changes and cumulative reductions from launch prices
  - Identifies significant price cuts and tracks pricing duration patterns

  Data characteristics (current state):
  - 104 pricing records covering 103 unique models across 10+ AI providers
  - 99% current pricing records, minimal historical price change data available
  - Input pricing ranges: $0.035 - $150 per million tokens
  - Output pricing typically 2-10x higher than input costs
  - Most models show zero cumulative reduction (launch pricing still active)
  - One historical price change detected showing significant increases (+92.9% input, +292.9% output)

  This is foundational data for competitive intelligence, cost optimization strategies,
  and tracking the broader AI market dynamics where frontier capabilities become commodity-priced.

  Business applications:
  - Cost forecasting for AI infrastructure planning
  - Vendor price competitiveness analysis
  - Market timing for model adoption decisions
  - Intelligence deflation research and trend analysis
connection: bruin-playground-arsalan
tags:
  - ai-price-wars
  - pricing-analysis
  - intelligence-deflation
  - llm-market
  - competitive-intelligence
  - cost-optimization
  - price-evolution
  - market-dynamics

materialization:
  type: table
  strategy: create+replace

depends:
  - raw.llm_price_history

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: model_id
    type: VARCHAR
    description: |
      Unique model identifier serving as natural key (e.g., gpt-4, claude-3-opus, llama-3-70b).
      103 distinct values tracked. Format varies by vendor but follows provider-model-variant pattern.
      Used for joining with model quality benchmarks and capability classifications.
    primary_key: true
    checks:
      - name: not_null
  - name: vendor
    type: VARCHAR
    description: |
      Standardized AI provider name with proper capitalization (OpenAI, Anthropic, Google, Amazon,
      Mistral, DeepSeek, Meta, Cohere). Normalized from raw lowercase vendor names for consistent
      display and grouping. 10 distinct vendors currently tracked in the competitive landscape.
    checks:
      - name: not_null
  - name: model_name
    type: VARCHAR
    description: |
      Human-readable model display name as marketed by the provider. Often includes capability
      tiers (turbo, pro, ultra, mini) or version identifiers. More descriptive than model_id
      for dashboards and user-facing analysis. 103+ distinct names reflecting model variants.
    checks:
      - name: not_null
  - name: price_input_per_mtok
    type: DOUBLE
    description: |
      Input token cost in USD per million tokens for this pricing period. Foundation metric for
      cost-per-query calculations. Current market ranges from $0.035 (commodity models) to
      $150 (specialized/premium models). Average ~$5.73. Essential for usage-based cost modeling.
    checks:
      - name: not_null
  - name: price_output_per_mtok
    type: DOUBLE
    description: |
      Output token generation cost in USD per million tokens. Typically 2-10x higher than input
      due to generation complexity. Ranges from $0.04 to $600. Average ~$24.38. Critical component
      for conversation cost calculations and model ROI analysis.
    checks:
      - name: not_null
  - name: price_blended_per_mtok
    type: DOUBLE
    description: |
      Industry-standard blended pricing using 75% input + 25% output token weighting, reflecting
      typical conversation patterns where input context dominates. Computed as:
      (input_price * 0.75) + (output_price * 0.25). Used for simplified cost comparisons
      across models without detailed token ratio analysis.
    checks:
      - name: not_null
  - name: from_date
    type: DATE
    description: |
      Start date when this pricing period became effective. Null for 99%+ of records (original
      launch pricing still active). When populated, indicates a pricing change event. Critical
      for time-series analysis of market dynamics and competitive responses.
    primary_key: true
  - name: to_date
    type: DATE
    description: |
      End date of pricing period validity. Null for current active pricing (99%+ of records).
      When populated alongside from_date, marks a completed historical pricing period. Essential
      for understanding pricing duration and market stability patterns.
  - name: is_current_price
    type: BOOLEAN
    description: |
      Flag indicating active pricing status (to_date is null). True for 99%+ of records reflecting
      market recency where most models maintain launch pricing. False indicates superseded
      historical pricing. Key filter for current market analysis vs historical comparisons.
    checks:
      - name: not_null
  - name: vendor_normalized
    type: VARCHAR
    description: |
      Original lowercase vendor identifier preserved from source data. Used for technical
      operations and maintaining source data traceability. Matches raw data vendor field
      exactly while vendor column provides display-ready formatting.
    checks:
      - name: not_null
  - name: price_period_days
    type: INTEGER
    description: |
      Duration of pricing period in calendar days. Computed from from_date to to_date (or current
      date if active). Null for most records due to absent from_date. When available, indicates
      pricing stability duration - useful for predicting future price change timing patterns.
  - name: prev_input_price
    type: DOUBLE
    description: |
      Previous input pricing for this model used in change calculations via LAG window function.
      Null for most records due to limited historical data availability. When populated,
      enables period-over-period analysis and price elasticity studies.
  - name: prev_output_price
    type: DOUBLE
    description: |
      Previous output pricing for this model from the preceding pricing period. Rare non-null
      values due to market recency. Essential for computing output price volatility and
      understanding generation cost trends versus input cost patterns.
  - name: input_price_change_pct
    type: DOUBLE
    description: |
      Percentage change in input pricing from previous period ((current - previous) / previous * 100).
      Positive values indicate price increases, negative indicate reductions. Null for most records
      lacking historical comparison. Current data shows +92.9% increase for one model indicating
      significant price adjustments can occur.
  - name: output_price_change_pct
    type: DOUBLE
    description: |
      Percentage change in output token pricing from previous period. Similar calculation as input
      change but often shows different patterns due to generation cost dynamics. Current data
      shows one model with +292.9% output increase, suggesting output pricing more volatile.
  - name: is_price_cut
    type: BOOLEAN
    description: |
      Flag identifying pricing reductions (input price < previous input price). All current records
      show false, indicating no price cuts detected in available historical data. Critical metric
      for tracking the "intelligence deflation" hypothesis where AI costs should decrease over time.
    checks:
      - name: not_null
  - name: cumulative_input_reduction_pct
    type: DOUBLE
    description: |
      Total percentage reduction from model's original launch input price to current price.
      Computed as (current - launch) / launch * 100. Most values near zero reflect limited
      price evolution since launch. Key metric for quantifying intelligence deflation magnitude
      and tracking long-term cost reduction trends.
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

with_vendor AS (
    SELECT
        model_id,
        model_name,
        CASE
            WHEN vendor = 'openai' THEN 'OpenAI'
            WHEN vendor = 'anthropic' THEN 'Anthropic'
            WHEN vendor = 'google' THEN 'Google'
            WHEN vendor = 'amazon' THEN 'Amazon'
            WHEN vendor = 'mistral' THEN 'Mistral'
            WHEN vendor = 'deepseek' THEN 'DeepSeek'
            WHEN vendor = 'meta' THEN 'Meta'
            WHEN vendor = 'cohere' THEN 'Cohere'
            ELSE INITCAP(vendor)
        END AS vendor,
        vendor AS vendor_normalized,
        price_input_per_mtok,
        price_output_per_mtok,
        ROUND(price_input_per_mtok * 0.75 + COALESCE(price_output_per_mtok, 0) * 0.25, 4) AS price_blended_per_mtok,
        from_date,
        to_date,
        is_current_price,
        -- Duration of pricing period
        CASE
            WHEN from_date IS NOT NULL AND to_date IS NOT NULL
            THEN DATE_DIFF(to_date, from_date, DAY)
            WHEN from_date IS NOT NULL AND to_date IS NULL
            THEN DATE_DIFF(CURRENT_DATE(), from_date, DAY)
            ELSE NULL
        END AS price_period_days
    FROM deduped
),

with_changes AS (
    SELECT
        *,
        LAG(price_input_per_mtok) OVER (PARTITION BY model_id ORDER BY from_date ASC NULLS FIRST) AS prev_input_price,
        LAG(price_output_per_mtok) OVER (PARTITION BY model_id ORDER BY from_date ASC NULLS FIRST) AS prev_output_price,
        FIRST_VALUE(price_input_per_mtok) OVER (PARTITION BY model_id ORDER BY from_date ASC NULLS FIRST) AS launch_input_price
    FROM with_vendor
)

SELECT
    model_id,
    vendor,
    model_name,
    price_input_per_mtok,
    price_output_per_mtok,
    price_blended_per_mtok,
    from_date,
    to_date,
    is_current_price,
    vendor_normalized,
    price_period_days,
    prev_input_price,
    prev_output_price,

    -- Price change percentages
    CASE
        WHEN prev_input_price IS NOT NULL AND prev_input_price > 0
        THEN ROUND((price_input_per_mtok - prev_input_price) / prev_input_price * 100, 1)
        ELSE NULL
    END AS input_price_change_pct,

    CASE
        WHEN prev_output_price IS NOT NULL AND prev_output_price > 0
        THEN ROUND((price_output_per_mtok - prev_output_price) / prev_output_price * 100, 1)
        ELSE NULL
    END AS output_price_change_pct,

    -- Is this a price cut?
    CASE
        WHEN prev_input_price IS NOT NULL AND price_input_per_mtok < prev_input_price THEN TRUE
        ELSE FALSE
    END AS is_price_cut,

    -- Cumulative reduction from launch price
    CASE
        WHEN launch_input_price IS NOT NULL AND launch_input_price > 0
        THEN ROUND((price_input_per_mtok - launch_input_price) / launch_input_price * 100, 1)
        ELSE NULL
    END AS cumulative_input_reduction_pct

FROM with_changes
ORDER BY vendor, model_id, from_date ASC NULLS FIRST
