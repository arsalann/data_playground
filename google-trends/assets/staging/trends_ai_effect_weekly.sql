/* @bruin
name: staging.trends_ai_effect_weekly
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Monthly search trend data for a curated set of terms, normalized to an
  index of 100 at the pre-ChatGPT baseline (Jul-Nov 2022). Terms are chosen
  to represent different query intents: factual lookups that LLMs can handle
  (prices, weather, stocks), entity/discovery searches that still need Google,
  and the "ai" term itself as a barometer of the AI revolution.

  Edge months are trimmed (starts Apr 2021, ends Nov 2025) to avoid
  data collection artifacts.

depends:
  - raw.google_trends_us

materialization:
  type: table
  strategy: create+replace

columns:
  - name: month
    type: DATE
    description: First day of the month
    primary_key: true
    nullable: false
  - name: term
    type: VARCHAR
    description: Google search term
    nullable: false
  - name: query_intent
    type: VARCHAR
    description: |
      Query intent category: 'lookup' (prices, weather, stocks — LLM-replaceable),
      'entity' (brands, sports, entertainment — needs Google), or 'ai_barometer'
  - name: avg_score
    type: DOUBLE
    description: Average per-DMA score for this term-month (controls for varying DMA coverage)
  - name: national_score
    type: INTEGER
    description: Sum of scores across all DMAs
  - name: dma_count
    type: INTEGER
    description: Number of DMA-week entries with a score
  - name: baseline_avg
    type: DOUBLE
    description: Average per-DMA score during baseline period (Jul-Nov 2022)
  - name: indexed_score
    type: DOUBLE
    description: Score indexed to baseline=100 (values above 100 mean growth vs baseline)

@bruin */

WITH curated_terms AS (
    SELECT term, query_intent FROM UNNEST([
        STRUCT('ai' AS term, 'ai_barometer' AS query_intent),
        STRUCT('bitcoin price', 'lookup'),
        STRUCT('ethereum price', 'lookup'),
        STRUCT('current weather', 'lookup'),
        STRUCT('amazon stock', 'lookup'),
        STRUCT('aapl stock', 'lookup'),
        STRUCT('meta stock', 'lookup'),
        STRUCT('stock market news today', 'lookup'),
        STRUCT('movies', 'entity'),
        STRUCT('real madrid', 'entity'),
        STRUCT('champions league', 'entity'),
        STRUCT('nfl', 'entity'),
        STRUCT('community', 'entity'),
        STRUCT('church', 'entity'),
        STRUCT('northern lights', 'entity'),
        STRUCT('capcut', 'entity'),
        STRUCT('palantir stock', 'lookup'),
        STRUCT('accident attorney', 'entity'),
        STRUCT('silver price', 'lookup'),
        STRUCT('xrp', 'lookup')
    ])
),

monthly AS (
    SELECT
        DATE_TRUNC(week, MONTH) AS month,
        u.term,
        c.query_intent,
        ROUND(AVG(avg_score), 2) AS avg_score,
        SUM(national_score) AS national_score,
        SUM(dma_coverage) AS dma_count
    FROM raw.google_trends_us u
    INNER JOIN curated_terms c ON u.term = c.term
    WHERE u.national_score > 0
      AND u.week >= '2021-04-01'
      AND u.week < '2025-12-01'
    GROUP BY 1, 2, 3
),

baselines AS (
    SELECT
        term,
        AVG(avg_score) AS baseline_avg
    FROM monthly
    WHERE month >= '2022-07-01' AND month < '2022-12-01'
    GROUP BY term
)

SELECT
    m.month,
    m.term,
    m.query_intent,
    m.avg_score,
    m.national_score,
    m.dma_count,
    b.baseline_avg,
    ROUND(m.avg_score / NULLIF(b.baseline_avg, 0) * 100, 1) AS indexed_score
FROM monthly m
INNER JOIN baselines b ON m.term = b.term
ORDER BY m.term, m.month
