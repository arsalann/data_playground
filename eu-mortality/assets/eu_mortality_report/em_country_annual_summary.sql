/* @bruin

name: eu_mortality_report.em_country_annual_summary
type: bq.sql
description: |
  Country-level annual summary of heat-attributable excess weekly deaths,
  rolled up from the NUTS3-week heat attribution panel.

  One row per country × calendar year. Used by the dashboard hero counters and
  the per-country annual trend chart.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - eu_mortality_staging.em_heat_attribution
  - eu_mortality_staging.em_population_dim
  - eu_mortality_staging.em_nuts3_dim

tags:
  - eu-27
  - mortality
  - report
  - country

columns:
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-2 country code.
    primary_key: true
    checks:
      - name: not_null
  - name: country_name_en
    type: VARCHAR
    description: English country name.
  - name: year
    type: INTEGER
    description: Calendar year (ISO).
    primary_key: true
    checks:
      - name: not_null
  - name: total_excess_deaths
    type: DOUBLE
    description: Sum of weekly excess deaths across the year (positive and negative).
  - name: heat_attributable_excess
    type: DOUBLE
    description: Sum of positive excess deaths during heat weeks (anomaly > 2 deg C).
  - name: heat_weeks
    type: INTEGER
    description: Number of NUTS3 x ISO-week combinations classified as heat weeks.
  - name: pop_total
    type: DOUBLE
    description: Country population at year-start (sum of NUTS3 totals).
  - name: heat_deaths_per_100k
    type: DOUBLE
    description: heat_attributable_excess / pop_total * 100,000.

@bruin */

WITH a AS (
    SELECT
        country_code,
        country_name_en,
        iso_year AS year,
        SUM(IFNULL(excess_deaths, 0)) AS total_excess_deaths,
        SUM(IFNULL(heat_attributable_excess, 0)) AS heat_attributable_excess,
        COUNTIF(is_heat_week) AS heat_weeks
    FROM `bruin-playground-arsalan.eu_mortality_staging.em_heat_attribution`
    WHERE country_code IS NOT NULL
    GROUP BY country_code, country_name_en, iso_year
),

p AS (
    SELECT
        d.country_code,
        h.iso_year AS year,
        SUM(p.pop_total) AS pop_total
    FROM `bruin-playground-arsalan.eu_mortality_staging.em_population_dim` p
    JOIN `bruin-playground-arsalan.eu_mortality_staging.em_nuts3_dim` d USING (nuts_id)
    JOIN UNNEST(GENERATE_ARRAY(2015, 2025)) AS h_iso_year ON p.ref_year = h_iso_year
    JOIN (SELECT DISTINCT iso_year FROM `bruin-playground-arsalan.eu_mortality_staging.em_heat_attribution`) h
        ON h.iso_year = p.ref_year
    GROUP BY d.country_code, h.iso_year
)

SELECT
    a.country_code,
    a.country_name_en,
    a.year,
    a.total_excess_deaths,
    a.heat_attributable_excess,
    a.heat_weeks,
    p.pop_total,
    SAFE_DIVIDE(a.heat_attributable_excess, p.pop_total) * 100000 AS heat_deaths_per_100k
FROM a
LEFT JOIN p ON p.country_code = a.country_code AND p.year = a.year
ORDER BY a.country_code, a.year
