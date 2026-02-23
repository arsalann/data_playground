/* @bruin
name: raw.google_trends_breakouts
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Combines US and international rising/breakout search terms into a single table.
  Rising terms are searches experiencing explosive growth, measured by percent_gain.
  Aggregated to (week, term, source) level with the number of regions where the
  term is breaking out and the maximum percent gain observed.

  Data sources:
    - bigquery-public-data.google_trends.top_rising_terms (US DMAs)
    - bigquery-public-data.google_trends.international_top_rising_terms (42 countries)
  License: Google Terms of Service

materialization:
  type: table
  strategy: create+replace

columns:
  - name: week
    type: DATE
    description: Start date of the weekly period
    primary_key: true
  - name: term
    type: VARCHAR
    description: Breakout search term
  - name: source
    type: VARCHAR
    description: Origin of the data — 'us' for US DMAs or 'international' for global
  - name: max_percent_gain
    type: INTEGER
    description: Highest percent gain observed across all regions for this term-week
  - name: avg_percent_gain
    type: DOUBLE
    description: Average percent gain across regions where this term was rising
  - name: num_regions
    type: INTEGER
    description: Number of distinct regions (DMAs or countries) where this term was rising
  - name: avg_score
    type: DOUBLE
    description: Average score across regions where score was available
  - name: best_rank
    type: INTEGER
    description: Best (lowest) rank achieved in any region
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when this data was materialized

@bruin */

WITH us_rising AS (
    SELECT
        week,
        term,
        'us' AS source,
        percent_gain,
        score,
        rank,
        CAST(dma_id AS STRING) AS region_id
    FROM `bigquery-public-data.google_trends.top_rising_terms`
    WHERE week IS NOT NULL
      AND score IS NOT NULL
),

intl_rising AS (
    SELECT
        week,
        term,
        'international' AS source,
        percent_gain,
        score,
        rank,
        country_code AS region_id
    FROM `bigquery-public-data.google_trends.international_top_rising_terms`
    WHERE week IS NOT NULL
      AND score IS NOT NULL
)

SELECT
    week,
    term,
    source,
    MAX(percent_gain) AS max_percent_gain,
    ROUND(AVG(percent_gain), 0) AS avg_percent_gain,
    COUNT(DISTINCT region_id) AS num_regions,
    ROUND(AVG(score), 2) AS avg_score,
    MIN(rank) AS best_rank,
    CURRENT_TIMESTAMP() AS extracted_at
FROM (
    SELECT * FROM us_rising
    UNION ALL
    SELECT * FROM intl_rising
)
GROUP BY week, term, source
ORDER BY week, max_percent_gain DESC
