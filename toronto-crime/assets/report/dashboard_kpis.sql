/* @bruin
name: report.dashboard_kpis
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Dashboard-ready KPI table for Toronto CSI rows, latest complete year rows,
  year-over-year change, and City of Toronto population-adjusted rates.

depends:
  - staging.crime_events
  - staging.neighbourhood_profiles

materialization:
  type: table
  strategy: create+replace

columns:
  - name: metric_name
    type: VARCHAR
    description: Stable metric identifier used by DAC widgets.
    primary_key: true
  - name: metric_value
    type: DOUBLE
    description: Numeric metric value.
  - name: metric_label
    type: VARCHAR
    description: Human-readable metric label with units.
  - name: latest_complete_year
    type: INTEGER
    description: Latest year treated as complete for yearly comparisons.
  - name: comparison_year
    type: INTEGER
    description: Prior year used for year-over-year comparison.
  - name: year_over_year_change_pct
    type: DOUBLE
    description: Year-over-year percent change for the latest complete year.
  - name: source_updated_at
    type: TIMESTAMP
    description: Most recent extraction timestamp from the staged CSI rows.
  - name: population_denominator
    type: INTEGER
    description: City of Toronto population denominator from the 2021 158-neighbourhood profile model.
  - name: population_scope
    type: VARCHAR
    description: Geography represented by the population denominator.

@bruin */

WITH latest_year AS (
    SELECT COALESCE(
        MAX(IF(occurrence_year < EXTRACT(YEAR FROM CURRENT_DATE()), occurrence_year, NULL)),
        MAX(occurrence_year)
    ) AS latest_complete_year
    FROM staging.crime_events
),

yearly AS (
    SELECT
        occurrence_year,
        COUNT(*) AS crime_count
    FROM staging.crime_events
    GROUP BY 1
),

population AS (
    SELECT
        SUM(population) AS city_population,
        'City of Toronto 158-neighbourhood model, 2021 Census profile; not GTA' AS population_scope
    FROM staging.neighbourhood_profiles
    WHERE neighbourhood_model = 158
      AND profile_year = 2021
),

base AS (
    SELECT
        ly.latest_complete_year,
        ly.latest_complete_year - 1 AS comparison_year,
        (SELECT COUNT(*) FROM staging.crime_events) AS total_rows,
        latest.crime_count AS latest_year_rows,
        prior.crime_count AS prior_year_rows,
        SAFE_DIVIDE(latest.crime_count - prior.crime_count, prior.crime_count) * 100 AS yoy_change_pct,
        p.city_population,
        p.population_scope,
        SAFE_DIVIDE(latest.crime_count, p.city_population) * 1000 AS citywide_rate_per_1000,
        SAFE_DIVIDE(latest.crime_count, p.city_population) * 100000 AS citywide_rate_per_100k,
        (SELECT MAX(extracted_at) FROM staging.crime_events) AS source_updated_at
    FROM latest_year AS ly
    LEFT JOIN yearly AS latest
        ON ly.latest_complete_year = latest.occurrence_year
    LEFT JOIN yearly AS prior
        ON ly.latest_complete_year - 1 = prior.occurrence_year
    CROSS JOIN population AS p
)

SELECT
    'total_csi_rows' AS metric_name,
    CAST(total_rows AS FLOAT64) AS metric_value,
    'Total CSI rows, all years' AS metric_label,
    latest_complete_year,
    comparison_year,
    ROUND(yoy_change_pct, 2) AS year_over_year_change_pct,
    source_updated_at,
    city_population AS population_denominator,
    population_scope
FROM base

UNION ALL

SELECT
    'latest_complete_year_rows' AS metric_name,
    CAST(latest_year_rows AS FLOAT64) AS metric_value,
    CONCAT('CSI rows in ', CAST(latest_complete_year AS STRING)) AS metric_label,
    latest_complete_year,
    comparison_year,
    ROUND(yoy_change_pct, 2) AS year_over_year_change_pct,
    source_updated_at,
    city_population AS population_denominator,
    population_scope
FROM base

UNION ALL

SELECT
    'year_over_year_change_pct' AS metric_name,
    ROUND(yoy_change_pct, 2) AS metric_value,
    CONCAT('YoY change vs ', CAST(comparison_year AS STRING), ' (%)') AS metric_label,
    latest_complete_year,
    comparison_year,
    ROUND(yoy_change_pct, 2) AS year_over_year_change_pct,
    source_updated_at,
    city_population AS population_denominator,
    population_scope
FROM base

UNION ALL

SELECT
    'toronto_population_denominator' AS metric_name,
    CAST(city_population AS FLOAT64) AS metric_value,
    'City of Toronto population denominator, 2021' AS metric_label,
    latest_complete_year,
    comparison_year,
    ROUND(yoy_change_pct, 2) AS year_over_year_change_pct,
    source_updated_at,
    city_population AS population_denominator,
    population_scope
FROM base

UNION ALL

SELECT
    'citywide_rate_per_1000_people' AS metric_name,
    ROUND(citywide_rate_per_1000, 2) AS metric_value,
    CONCAT('City of Toronto CSI rows per 1,000 residents in ', CAST(latest_complete_year AS STRING)) AS metric_label,
    latest_complete_year,
    comparison_year,
    ROUND(yoy_change_pct, 2) AS year_over_year_change_pct,
    source_updated_at,
    city_population AS population_denominator,
    population_scope
FROM base

UNION ALL

SELECT
    'citywide_rate_per_100k_people' AS metric_name,
    ROUND(citywide_rate_per_100k, 2) AS metric_value,
    CONCAT('City of Toronto CSI rows per 100,000 residents in ', CAST(latest_complete_year AS STRING)) AS metric_label,
    latest_complete_year,
    comparison_year,
    ROUND(yoy_change_pct, 2) AS year_over_year_change_pct,
    source_updated_at,
    city_population AS population_denominator,
    population_scope
FROM base
