/* @bruin

name: eu_mortality_report.em_country_annual_summary
type: bq.sql
description: |
  Country-level annual summary of heat-attributable excess mortality for EU-27 member states.
  Aggregates NUTS3-level weekly data to provide country-year totals of excess deaths,
  specifically focusing on heat attribution during temperature anomaly periods.

  Data spans 2015-2025 using ISGlobal/Ballester methodology with 2015-2019 climatological
  baseline. Heat attribution applies only to weeks with temperature anomaly >+2°C above
  1991-2020 climatology, counting only positive excess deaths to avoid false attribution.

  One row per country × calendar year. Used by dashboard hero counters, annual trend
  charts, and comparative country analysis. Supports EU-wide mortality surveillance
  and climate health impact assessment.
connection: bruin-playground-arsalan
tags:
  - eu-27
  - mortality
  - report
  - country
  - heat-attribution
  - public-health
  - climate-health
  - annual-summary
  - time-series
  - excess-deaths
  - dashboard-mart

materialization:
  type: table
  strategy: create+replace

depends:
  - eu_mortality_staging.em_heat_attribution
  - eu_mortality_staging.em_population_dim
  - eu_mortality_staging.em_nuts3_dim

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-2 country code for EU-27 member states. Two-character identifier used throughout Eurostat and EU statistical systems.
    primary_key: true
    checks:
      - name: not_null
  - name: country_name_en
    type: VARCHAR
    description: English country name as provided by Eurostat. Standardized display name for dashboard visualization and human-readable reporting.
  - name: year
    type: INTEGER
    description: Calendar year (ISO 8601). Study period spans 2015-2025, where 2015-2019 serves as baseline period for excess mortality calculations.
    primary_key: true
    checks:
      - name: not_null
  - name: total_excess_deaths
    type: DOUBLE
    description: |
      Annual sum of weekly excess deaths across all NUTS3 regions in the country (count).
      Calculated as observed deaths minus 2015-2019 climatological expected deaths.
      Includes both positive excess (elevated mortality) and negative excess (below-expected mortality).
      Can be negative if overall mortality was below historical baseline.
  - name: heat_attributable_excess
    type: DOUBLE
    description: |
      Annual sum of excess deaths attributable to heat events (count).
      Only counts positive excess deaths during weeks with temperature anomaly >+2°C above
      1991-2020 climatology. Conservative attribution method excludes negative excess to
      avoid false heat attribution. Zero when no qualifying heat weeks or no positive excess.
  - name: heat_weeks
    type: INTEGER
    description: |
      Total count of NUTS3 × ISO-week combinations classified as heat weeks during the year.
      Heat week definition: temperature anomaly >+2°C above 1991-2020 climatology.
      Higher values indicate more widespread or persistent heat events across the country.
  - name: pop_total
    type: DOUBLE
    description: |
      Total country population for the year (persons). Aggregated from Eurostat
      NUTS3-level population data. Used as denominator for per-capita mortality rates.
      Source: Eurostat demo_r_pjangrp3.
    checks:
      - name: not_null
  - name: heat_deaths_per_100k
    type: DOUBLE
    description: |
      Heat-attributable excess deaths per 100,000 inhabitants (rate).
      Calculated as: (heat_attributable_excess / pop_total) × 100,000.
      Standardized mortality rate enabling cross-country comparison regardless of population size.
      Zero when no heat-attributable deaths identified.

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
