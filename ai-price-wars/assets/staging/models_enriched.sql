/* @bruin

name: staging.models_enriched
type: bq.sql
description: |
  Unified model catalog joining OpenRouter current pricing with Arena ELO rankings.
  Deduplicates raw data, normalizes provider names, classifies models by tier and
  category, and computes price/performance metrics. This is the primary analysis
  table for the AI price wars project.

  Central table for analyzing "intelligence deflation" - the rapid decline in AI model
  costs while quality improves. Enables price/performance analysis, competitive
  dynamics tracking, and democratization of AI capabilities.

  Data characteristics:
  - 352 total models with 348 distinct identifiers (some duplicates exist)
  - Arena metrics are sparse: only ~21 models have text ELO, ~36 have code ELO
  - Pricing data uses -1000000 to encode missing/unavailable prices
  - Context lengths range from small (4K) to massive (2M+ tokens)
  - Model creation dates span from 2023-05-28 to present
  - Mix of free (28 models) and paid models with wide pricing variation
connection: bruin-playground-arsalan
tags:
  - ai-market-analysis
  - pricing-intelligence
  - model-performance
  - arena-rankings
  - price-wars
  - staging

materialization:
  type: table
  strategy: create+replace

depends:
  - raw.openrouter_models
  - raw.arena_leaderboard

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: model_id
    type: VARCHAR
    description: |
      OpenRouter model identifier (provider/model-name format, e.g. openai/gpt-4o).
      Note: Contains 348 unique values out of 352 rows, indicating some duplicate
      entries exist in the source data. Used as primary identifier for joins with
      pricing history and performance benchmarks.
    checks:
      - name: not_null
  - name: model_name
    type: VARCHAR
    description: |
      Human-readable model name as displayed to users. Length varies from 9-54
      characters. Often includes version numbers, size indicators, or capability
      hints (e.g. "thinking", "preview").
    checks:
      - name: not_null
  - name: provider
    type: VARCHAR
    description: |
      Normalized provider name (OpenAI, Anthropic, Google, Meta, etc.). Standardized
      from raw provider strings to enable consistent analysis. 56 distinct providers
      represented, ranging from frontier labs to emerging AI companies.
    checks:
      - name: not_null
  - name: price_input_per_mtok
    type: DOUBLE
    description: |
      Cost per million input tokens in USD. Range: -1000000 to $150/MTok.
      Negative values (-1000000) indicate missing/unavailable pricing data.
      Most models fall in $0-50/MTok range with frontier models at premium pricing.
    checks:
      - name: not_null
  - name: price_output_per_mtok
    type: DOUBLE
    description: |
      Cost per million output tokens in USD. Range: -1000000 to $600/MTok.
      Output tokens typically cost 2-4x input tokens. Negative values encode
      missing data. Ultra-premium models can exceed $100/MTok for outputs.
    checks:
      - name: not_null
  - name: price_blended_per_mtok
    type: DOUBLE
    description: |
      Blended price using 3:1 input:output ratio (0.75 * input + 0.25 * output).
      Represents typical chat workload cost. Range: -1000000 to $262.50/MTok.
      Used for tier classification and price/performance analysis.
    checks:
      - name: not_null
  - name: context_length
    type: INTEGER
    description: |
      Maximum context window in tokens. Average: ~276K tokens with high variance
      (up to 2M+ for long-context models). Critical for document processing and
      complex reasoning tasks. Larger contexts typically command premium pricing.
    checks:
      - name: not_null
  - name: is_free
    type: BOOLEAN
    description: |
      Whether the model is completely free to use (28 out of 352 models).
      Free models are often open-source or have usage-based limitations.
      Important for cost-sensitive applications and research access.
    checks:
      - name: not_null
  - name: model_created_at
    type: TIMESTAMP
    description: |
      When the model was first available on OpenRouter. Date range: 2023-05-28
      to 2026-04-03. Enables tracking model release cadence and market evolution.
      314 distinct timestamps suggest frequent model launches.
    checks:
      - name: not_null
  - name: provider_tier
    type: VARCHAR
    description: |
      Provider classification based on market position and capabilities:
      - Frontier: OpenAI, Anthropic, Google (cutting-edge research labs)
      - Major Open Source: Meta, Mistral, DeepSeek (large-scale open models)
      - Challenger: xAI, Cohere (specialized competitors)
      - Cloud Provider: Amazon, Microsoft (cloud platform models)
      - Emerging: Smaller companies and new entrants
    checks:
      - name: not_null
  - name: price_tier
    type: VARCHAR
    description: |
      Price classification based on blended pricing:
      - Free: $0 (28 models, 8% of total)
      - Budget: <$1/MTok (cost-effective options)
      - Mid: $1-10/MTok (mainstream pricing)
      - Premium: $10-50/MTok (high-performance models)
      - Ultra: >$50/MTok (frontier capabilities with premium cost)
    checks:
      - name: not_null
  - name: model_family
    type: VARCHAR
    description: |
      Model family extracted from model name (GPT-4, Claude, Gemini, Llama, etc.).
      21 distinct families represented, enabling analysis of competitive dynamics
      within model lineages. Useful for tracking family-specific pricing strategies
      and capability evolution.
    checks:
      - name: not_null
  - name: is_reasoning_model
    type: BOOLEAN
    description: |
      Whether this is a reasoning/thinking model (o1, o3, thinking variants).
      30 out of 352 models (8.5%) are reasoning-capable. These models typically
      show higher latency but better performance on complex analytical tasks.
    checks:
      - name: not_null
  - name: arena_text_elo
    type: INTEGER
    description: |
      ELO score on Arena text/chat leaderboard based on human preference votes.
      Only 21 models (~6%) have text ELO scores, indicating limited Arena coverage.
      Average score: ~1460 ELO. Higher scores indicate better human-perceived quality.
      Null for most models due to sparse Arena participation.
  - name: arena_text_rank
    type: INTEGER
    description: |
      Rank on Arena text/chat leaderboard (1 = best). Average rank: ~25 among
      ranked models. Sparse data (331 nulls out of 352). Useful for identifying
      top-tier models but limited coverage reduces analytical utility.
  - name: arena_code_elo
    type: INTEGER
    description: |
      ELO score on Arena code leaderboard. 36 models (~10%) have code ELO scores.
      Average: ~1370 ELO with higher variance than text scores. Critical for
      developer-focused model selection and coding capability assessment.
  - name: arena_code_rank
    type: INTEGER
    description: |
      Rank on Arena code leaderboard. Average rank: ~28 among ranked models.
      Higher coverage than text rankings but still sparse (316 nulls).
      Lower numbers indicate better coding performance.
  - name: price_per_elo_point
    type: DOUBLE
    description: |-
      Cost efficiency metric: blended price divided by text ELO score, multiplied
      by 1000 for readability. Lower values indicate better value per quality unit.
      Range: $0.39 to $20.73 per ELO point. Only available for 21 models with
      both pricing and ELO data. Key metric for price/performance optimization.

@bruin */

WITH models_deduped AS (
    SELECT *
    FROM raw.openrouter_models
    WHERE model_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY model_id ORDER BY extracted_at DESC) = 1
),

-- Dedup arena by match_key (not model_name) to prevent cartesian joins.
-- Multiple arena entries (e.g. claude-opus-4-6 vs claude-opus-4-6-thinking)
-- can normalize to the same key. Take the best ELO per match_key.
arena_text AS (
    SELECT
        match_key,
        MAX(elo_score) AS elo_score,
        MIN(rank) AS rank
    FROM (
        SELECT
            elo_score,
            rank,
            LOWER(REGEXP_REPLACE(REGEXP_REPLACE(model_name, r'-thinking.*$|-\d{8}$', ''), r'[.\-:]', '')) AS match_key
        FROM raw.arena_leaderboard
        WHERE category = 'text'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY model_name, category ORDER BY extracted_at DESC) = 1
    )
    GROUP BY match_key
),

arena_code AS (
    SELECT
        match_key,
        MAX(elo_score) AS elo_score,
        MIN(rank) AS rank
    FROM (
        SELECT
            elo_score,
            rank,
            LOWER(REGEXP_REPLACE(REGEXP_REPLACE(model_name, r'-thinking.*$|-\d{8}$', ''), r'[.\-:]', '')) AS match_key
        FROM raw.arena_leaderboard
        WHERE category = 'code'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY model_name, category ORDER BY extracted_at DESC) = 1
    )
    GROUP BY match_key
),

enriched AS (
    SELECT
        m.model_id,
        m.model_name,

        -- Normalize provider names
        CASE
            WHEN m.provider IN ('openai') THEN 'OpenAI'
            WHEN m.provider IN ('anthropic') THEN 'Anthropic'
            WHEN m.provider IN ('google') THEN 'Google'
            WHEN m.provider IN ('meta-llama', 'meta') THEN 'Meta'
            WHEN m.provider IN ('mistralai', 'mistral') THEN 'Mistral'
            WHEN m.provider IN ('x-ai', 'xai') THEN 'xAI'
            WHEN m.provider IN ('deepseek') THEN 'DeepSeek'
            WHEN m.provider IN ('amazon') THEN 'Amazon'
            WHEN m.provider IN ('microsoft') THEN 'Microsoft'
            WHEN m.provider IN ('cohere') THEN 'Cohere'
            WHEN m.provider IN ('qwen') THEN 'Qwen'
            WHEN m.provider IN ('nvidia') THEN 'NVIDIA'
            ELSE INITCAP(m.provider)
        END AS provider,

        COALESCE(m.price_input_per_mtok, 0) AS price_input_per_mtok,
        COALESCE(m.price_output_per_mtok, 0) AS price_output_per_mtok,

        -- Blended price: assumes 3:1 input:output ratio (typical for chat)
        ROUND(COALESCE(m.price_input_per_mtok, 0) * 0.75 + COALESCE(m.price_output_per_mtok, 0) * 0.25, 4) AS price_blended_per_mtok,

        m.context_length,
        COALESCE(m.is_free, FALSE) AS is_free,
        m.model_created_at,

        -- Provider tier
        CASE
            WHEN m.provider IN ('openai') THEN 'Frontier'
            WHEN m.provider IN ('anthropic') THEN 'Frontier'
            WHEN m.provider IN ('google') THEN 'Frontier'
            WHEN m.provider IN ('meta-llama', 'meta') THEN 'Major Open Source'
            WHEN m.provider IN ('mistralai', 'mistral') THEN 'Major Open Source'
            WHEN m.provider IN ('deepseek') THEN 'Major Open Source'
            WHEN m.provider IN ('qwen') THEN 'Major Open Source'
            WHEN m.provider IN ('x-ai', 'xai') THEN 'Challenger'
            WHEN m.provider IN ('amazon') THEN 'Cloud Provider'
            WHEN m.provider IN ('microsoft') THEN 'Cloud Provider'
            WHEN m.provider IN ('cohere') THEN 'Challenger'
            ELSE 'Emerging'
        END AS provider_tier,

        -- Price tier
        CASE
            WHEN COALESCE(m.price_input_per_mtok, 0) = 0 AND COALESCE(m.price_output_per_mtok, 0) = 0 THEN 'Free'
            WHEN COALESCE(m.price_input_per_mtok, 0) * 0.75 + COALESCE(m.price_output_per_mtok, 0) * 0.25 < 1.0 THEN 'Budget (<$1/MTok)'
            WHEN COALESCE(m.price_input_per_mtok, 0) * 0.75 + COALESCE(m.price_output_per_mtok, 0) * 0.25 < 10.0 THEN 'Mid ($1-10/MTok)'
            WHEN COALESCE(m.price_input_per_mtok, 0) * 0.75 + COALESCE(m.price_output_per_mtok, 0) * 0.25 < 50.0 THEN 'Premium ($10-50/MTok)'
            ELSE 'Ultra (>$50/MTok)'
        END AS price_tier,

        -- Model family extraction
        CASE
            WHEN LOWER(m.model_id) LIKE '%gpt-4o%' THEN 'GPT-4o'
            WHEN LOWER(m.model_id) LIKE '%gpt-4.1%' THEN 'GPT-4.1'
            WHEN LOWER(m.model_id) LIKE '%gpt-4%' THEN 'GPT-4'
            WHEN LOWER(m.model_id) LIKE '%gpt-3.5%' THEN 'GPT-3.5'
            WHEN LOWER(m.model_id) LIKE '%o1%' OR LOWER(m.model_id) LIKE '%o3%' OR LOWER(m.model_id) LIKE '%o4%' THEN 'OpenAI Reasoning'
            WHEN LOWER(m.model_id) LIKE '%claude-opus%' THEN 'Claude Opus'
            WHEN LOWER(m.model_id) LIKE '%claude-sonnet%' OR LOWER(m.model_id) LIKE '%claude-4%' THEN 'Claude Sonnet'
            WHEN LOWER(m.model_id) LIKE '%claude-haiku%' THEN 'Claude Haiku'
            WHEN LOWER(m.model_id) LIKE '%claude-3%' THEN 'Claude 3.x'
            WHEN LOWER(m.model_id) LIKE '%gemini-2%' OR LOWER(m.model_id) LIKE '%gemini-3%' THEN 'Gemini'
            WHEN LOWER(m.model_id) LIKE '%gemini-1.5%' THEN 'Gemini 1.5'
            WHEN LOWER(m.model_id) LIKE '%gemma%' THEN 'Gemma'
            WHEN LOWER(m.model_id) LIKE '%llama-4%' THEN 'Llama 4'
            WHEN LOWER(m.model_id) LIKE '%llama-3%' THEN 'Llama 3.x'
            WHEN LOWER(m.model_id) LIKE '%mistral%' OR LOWER(m.model_id) LIKE '%mixtral%' THEN 'Mistral'
            WHEN LOWER(m.model_id) LIKE '%deepseek%' THEN 'DeepSeek'
            WHEN LOWER(m.model_id) LIKE '%grok%' THEN 'Grok'
            WHEN LOWER(m.model_id) LIKE '%qwen%' THEN 'Qwen'
            WHEN LOWER(m.model_id) LIKE '%phi%' THEN 'Phi'
            WHEN LOWER(m.model_id) LIKE '%command%' THEN 'Command'
            WHEN LOWER(m.model_id) LIKE '%nova%' THEN 'Nova'
            ELSE 'Other'
        END AS model_family,

        -- Reasoning model flag
        CASE
            WHEN LOWER(m.model_id) LIKE '%thinking%'
                OR LOWER(m.model_id) LIKE '%reason%'
                OR LOWER(m.model_name) LIKE '%thinking%'
                OR (m.provider = 'openai' AND (LOWER(m.model_id) LIKE '%/o1%' OR LOWER(m.model_id) LIKE '%/o3%' OR LOWER(m.model_id) LIKE '%/o4%'))
                OR LOWER(m.model_id) LIKE '%deepseek-r1%'
            THEN TRUE
            ELSE FALSE
        END AS is_reasoning_model,

        -- Arena scores (joined by fuzzy matching on model name)
        art.elo_score AS arena_text_elo,
        art.rank AS arena_text_rank,
        arc.elo_score AS arena_code_elo,
        arc.rank AS arena_code_rank

    FROM models_deduped m
    LEFT JOIN arena_text art ON (
        art.match_key = LOWER(REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(m.model_id, '/')[SAFE_OFFSET(1)], r'-thinking.*$|-\d{8}$', ''), r'[.\-:]', ''))
    )
    LEFT JOIN arena_code arc ON (
        arc.match_key = LOWER(REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(m.model_id, '/')[SAFE_OFFSET(1)], r'-thinking.*$|-\d{8}$', ''), r'[.\-:]', ''))
    )
)

SELECT
    *,
    CASE
        WHEN arena_text_elo IS NOT NULL AND price_blended_per_mtok > 0
        THEN ROUND(price_blended_per_mtok / arena_text_elo * 1000, 6)
        ELSE NULL
    END AS price_per_elo_point
FROM enriched
ORDER BY price_blended_per_mtok DESC
