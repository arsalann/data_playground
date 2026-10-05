/* @bruin

name: report.metro_country_cost_summary
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Country-level summary of included metro/urban-rail projects. Cost per
  kilometer is summarized as both a length-weighted average and an unweighted
  project median; the two measures answer different questions.

depends:
  - staging.metro_projects_enriched

materialization:
  type: table
  strategy: create+replace

columns:
  - name: country_code
    type: VARCHAR
    description: Two-letter country code.
    primary_key: true
  - name: country_name
    type: VARCHAR
    description: Country name.
  - name: analysis_region
    type: VARCHAR
    description: North America, Europe, or Other.
  - name: project_count
    type: INTEGER
    description: Number of included project or phase records.
    checks:
      - name: positive
  - name: total_route_km
    type: DOUBLE
    description: Total included corridor length in kilometers.
    checks:
      - name: positive
  - name: weighted_avg_cost_per_km_2025_usd_m
    type: DOUBLE
    description: Length-weighted average cost per kilometer in 2025 USD millions.
    checks:
      - name: positive
  - name: median_cost_per_km_2025_usd_m
    type: DOUBLE
    description: Median project cost per kilometer in 2025 USD millions.
  - name: weighted_avg_tunnel_pct
    type: DOUBLE
    description: Length-weighted tunnel share, percent.
  - name: weighted_avg_stations_per_km
    type: DOUBLE
    description: Length-weighted stations per kilometer.
  - name: median_duration_years
    type: DOUBLE
    description: Median construction duration in years among projects with valid dates.
  - name: gdp_per_capita_ppp_2021
    type: DOUBLE
    description: GDP per capita PPP context value near the project construction periods.
  - name: urban_population_pct
    type: DOUBLE
    description: Urban population percentage context value near the project construction periods.

@bruin */

WITH included AS (
    SELECT *
    FROM staging.metro_projects_enriched
    WHERE include_in_analysis = TRUE
      AND cost_per_km_2025_usd_m > 0
      AND length_km > 0
),
summarized AS (
    SELECT
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
        APPROX_QUANTILES(construction_duration_years, 100 IGNORE NULLS)[OFFSET(50)] AS median_duration_years,
        AVG(gdp_per_capita_ppp_2021) AS gdp_per_capita_ppp_2021,
        AVG(urban_population_pct) AS urban_population_pct
    FROM included
    GROUP BY country_code
)
SELECT *
FROM summarized
ORDER BY weighted_avg_cost_per_km_2025_usd_m DESC
