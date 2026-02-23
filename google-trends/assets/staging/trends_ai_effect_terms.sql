/* @bruin
name: staging.trends_ai_effect_terms
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Pre/post ChatGPT comparison for ALL terms in the dataset. Uses per-DMA
  average scores to normalize for coverage differences. Only includes terms
  active in both eras with at least 5 months of data in each.

  Edge months trimmed (Apr 2021 - Nov 2025) to avoid data artifacts.
  ChatGPT cutoff: December 2022.

depends:
  - raw.google_trends_us

materialization:
  type: table
  strategy: create+replace

columns:
  - name: term
    type: VARCHAR
    description: Google search term
    primary_key: true
    nullable: false
  - name: query_category
    type: VARCHAR
    description: |
      Query intent: 'informational' (how-to, what-is, meaning, recipe),
      'lookup' (price, weather, stock, score, results),
      or 'entity' (people, brands, events, sports)
  - name: is_llm_answerable
    type: BOOLEAN
    description: True if informational or lookup (the kind of query an LLM handles well)
  - name: pre_avg_score
    type: DOUBLE
    description: Average per-DMA score before Dec 2022
  - name: post_avg_score
    type: DOUBLE
    description: Average per-DMA score from Dec 2022 onward
  - name: pct_change
    type: DOUBLE
    description: Percentage change from pre to post average
  - name: pre_months
    type: INTEGER
    description: Number of months active before ChatGPT
  - name: post_months
    type: INTEGER
    description: Number of months active after ChatGPT

@bruin */

WITH monthly_avg AS (
    SELECT
        DATE_TRUNC(week, MONTH) AS month,
        term,
        AVG(avg_score) AS avg_score_per_dma
    FROM raw.google_trends_us
    WHERE national_score > 0
      AND week >= '2021-04-01'
      AND week < '2025-12-01'
    GROUP BY 1, 2
)

SELECT
    term,

    CASE
        WHEN LOWER(term) LIKE '%how to%' OR LOWER(term) LIKE '%what is%'
            OR LOWER(term) LIKE '%why %' OR LOWER(term) LIKE '%meaning%'
            OR LOWER(term) LIKE '%definition%' OR LOWER(term) LIKE '%recipe%'
            OR LOWER(term) LIKE '%tutorial%' OR LOWER(term) LIKE '%guide%'
        THEN 'informational'
        WHEN LOWER(term) LIKE '%price%' OR LOWER(term) LIKE '%stock%'
            OR LOWER(term) LIKE '%weather%' OR LOWER(term) LIKE '%score %'
            OR LOWER(term) LIKE '%results%' OR LOWER(term) LIKE '%schedule%'
            OR LOWER(term) LIKE '%today%' OR LOWER(term) LIKE '%live%'
            OR LOWER(term) LIKE '%news%' OR LOWER(term) LIKE '%update%'
        THEN 'lookup'
        ELSE 'entity'
    END AS query_category,

    CASE
        WHEN LOWER(term) LIKE '%how to%' OR LOWER(term) LIKE '%what is%'
            OR LOWER(term) LIKE '%why %' OR LOWER(term) LIKE '%meaning%'
            OR LOWER(term) LIKE '%definition%' OR LOWER(term) LIKE '%recipe%'
            OR LOWER(term) LIKE '%tutorial%' OR LOWER(term) LIKE '%guide%'
            OR LOWER(term) LIKE '%price%' OR LOWER(term) LIKE '%stock%'
            OR LOWER(term) LIKE '%weather%' OR LOWER(term) LIKE '%score %'
            OR LOWER(term) LIKE '%results%' OR LOWER(term) LIKE '%schedule%'
            OR LOWER(term) LIKE '%today%' OR LOWER(term) LIKE '%live%'
            OR LOWER(term) LIKE '%news%' OR LOWER(term) LIKE '%update%'
            OR LOWER(term) LIKE '%best %' OR LOWER(term) LIKE '%review%'
        THEN TRUE
        ELSE FALSE
    END AS is_llm_answerable,

    ROUND(AVG(CASE WHEN month < '2022-12-01' THEN avg_score_per_dma END), 2) AS pre_avg_score,
    ROUND(AVG(CASE WHEN month >= '2022-12-01' THEN avg_score_per_dma END), 2) AS post_avg_score,

    ROUND(
        (AVG(CASE WHEN month >= '2022-12-01' THEN avg_score_per_dma END)
         - AVG(CASE WHEN month < '2022-12-01' THEN avg_score_per_dma END))
        / NULLIF(AVG(CASE WHEN month < '2022-12-01' THEN avg_score_per_dma END), 0) * 100,
    1) AS pct_change,

    COUNTIF(month < '2022-12-01' AND avg_score_per_dma IS NOT NULL) AS pre_months,
    COUNTIF(month >= '2022-12-01' AND avg_score_per_dma IS NOT NULL) AS post_months

FROM monthly_avg
GROUP BY term
HAVING COUNTIF(month < '2022-12-01' AND avg_score_per_dma IS NOT NULL) >= 5
   AND COUNTIF(month >= '2022-12-01' AND avg_score_per_dma IS NOT NULL) >= 5
ORDER BY pct_change ASC
