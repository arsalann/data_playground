/* @bruin
name: raw.google_trends_us
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Aggregates Google Trends top search terms to a national (US) level per week.
  Each row represents one term in one week, with scores summed across all 210
  Designated Market Areas (DMAs). The DMA coverage count indicates how broadly
  a term is trending geographically.

  Data source: bigquery-public-data.google_trends.top_terms
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
    description: Google search term
  - name: national_score
    type: INTEGER
    description: Sum of relative popularity scores across all DMAs where the term was trending
  - name: dma_coverage
    type: INTEGER
    description: Number of distinct DMAs where this term had a non-null score
  - name: avg_score
    type: DOUBLE
    description: Average score across DMAs where the term was trending
  - name: avg_rank
    type: DOUBLE
    description: Average rank position across DMAs
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when this data was materialized

@bruin */

SELECT
    week,
    term,
    SUM(score) AS national_score,
    COUNTIF(score IS NOT NULL) AS dma_coverage,
    ROUND(AVG(CASE WHEN score IS NOT NULL THEN score END), 2) AS avg_score,
    ROUND(AVG(rank), 2) AS avg_rank,
    CURRENT_TIMESTAMP() AS extracted_at
FROM `bigquery-public-data.google_trends.top_terms`
WHERE week IS NOT NULL
GROUP BY week, term
HAVING national_score IS NOT NULL
ORDER BY week, national_score DESC
