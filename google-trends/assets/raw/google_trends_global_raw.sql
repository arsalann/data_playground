/* @bruin
name: raw.google_trends_global
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Aggregates Google Trends international top search terms to the country level
  per week. Only rows with a non-null score are included to keep the table
  focused on weeks where a term was actively trending in a country. Covers
  42 countries across all continents.

  Data source: bigquery-public-data.google_trends.international_top_terms
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
  - name: country_name
    type: VARCHAR
    description: Full country name (e.g. Germany, Japan, Brazil)
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-2 country code
  - name: num_regions
    type: INTEGER
    description: Number of sub-regions within the country where this term was trending
  - name: avg_score
    type: DOUBLE
    description: Average score across sub-regions within the country
  - name: max_score
    type: INTEGER
    description: Highest score in any sub-region for this term-week-country
  - name: avg_rank
    type: DOUBLE
    description: Average rank across sub-regions
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when this data was materialized

@bruin */

SELECT
    week,
    term,
    country_name,
    country_code,
    COUNT(DISTINCT region_code) AS num_regions,
    ROUND(AVG(score), 2) AS avg_score,
    MAX(score) AS max_score,
    ROUND(AVG(rank), 2) AS avg_rank,
    CURRENT_TIMESTAMP() AS extracted_at
FROM `bigquery-public-data.google_trends.international_top_terms`
WHERE week IS NOT NULL
  AND score IS NOT NULL
GROUP BY week, term, country_name, country_code
ORDER BY week, country_code, avg_score DESC
