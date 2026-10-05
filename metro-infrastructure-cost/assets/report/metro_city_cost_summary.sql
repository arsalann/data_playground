/* @bruin

name: report.metro_city_cost_summary
type: bq.sql
connection: bruin-playground-arsalan
description: |
  City or metropolitan-area summary of included metro/urban-rail projects.
  Project phases are aggregated within a city, with route-km weighting used
  for the average cost per kilometer and the unweighted median retained for
  comparison.

depends:
  - staging.metro_projects_enriched

materialization:
  type: table
  strategy: create+replace

columns:
  - name: city_key
    type: VARCHAR
    description: Stable country-and-city key for the aggregated city row.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: city_name
    type: VARCHAR
    description: City or metropolitan-area name.
  - name: country_code
    type: VARCHAR
    description: Two-letter country code.
  - name: country_name
    type: VARCHAR
    description: Country name for context, not the primary grouping level.
  - name: analysis_region
    type: VARCHAR
    description: North America, Europe, or Other.
  - name: project_count
    type: INTEGER
    description: Number of included project or phase records in the city.
    checks:
      - name: positive
  - name: total_route_km
    type: DOUBLE
    description: Total included corridor length in kilometers.
    checks:
      - name: positive
  - name: weighted_avg_cost_per_km_2025_usd_m
    type: DOUBLE
    description: Route-km-weighted average cost per kilometer in 2025 USD millions.
    checks:
      - name: positive
  - name: median_cost_per_km_2025_usd_m
    type: DOUBLE
    description: Unweighted median project cost per kilometer in 2025 USD millions.
  - name: weighted_avg_tunnel_pct
    type: DOUBLE
    description: Route-km-weighted tunnel share, percent.
  - name: weighted_avg_stations_per_km
    type: DOUBLE
    description: Route-km-weighted stations per kilometer.
  - name: median_duration_years
    type: DOUBLE
    description: Median construction duration in years among projects with valid dates.

@bruin */

WITH included AS (
    SELECT *
    FROM staging.metro_projects_enriched
    WHERE include_in_analysis = TRUE
      AND city IS NOT NULL
      AND city != ''
      AND cost_per_km_2025_usd_m > 0
      AND length_km > 0
),
summarized AS (
    SELECT
        CONCAT(country_code, '::', city) AS city_key,
        city AS city_name,
        country_code,
        ANY_VALUE(country_name) AS country_name,
        ANY_VALUE(analysis_region) AS analysis_region,
        COUNT(*) AS project_count,
        SUM(length_km) AS total_route_km,
        SAFE_DIVIDE(
            SUM(cost_per_km_2025_usd_m * length_km),
            SUM(length_km)
        ) AS weighted_avg_cost_per_km_2025_usd_m,
        APPROX_QUANTILES(cost_per_km_2025_usd_m, 100)[OFFSET(50)] AS median_cost_per_km_2025_usd_m,
        SAFE_DIVIDE(SUM(tunnel_pct * length_km), SUM(length_km)) AS weighted_avg_tunnel_pct,
        SAFE_DIVIDE(SUM(stations_per_km * length_km), SUM(length_km)) AS weighted_avg_stations_per_km,
        APPROX_QUANTILES(construction_duration_years, 100 IGNORE NULLS)[OFFSET(50)] AS median_duration_years
    FROM included
    GROUP BY city_key, city_name, country_code
)
SELECT *
FROM summarized
ORDER BY weighted_avg_cost_per_km_2025_usd_m DESC
