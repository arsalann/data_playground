/* @bruin

name: eu_mortality_staging.em_heat_attribution
type: bq.sql
description: |
  Heat-attributable excess weekly deaths per NUTS3 region.

  Joins:
    - em_mortality_panel (actual weekly deaths)
    - em_excess_baseline (2015-2019 expected deaths for the same NUTS3 + week-of-year)
    - em_temperature_panel (weekly temperature anomaly vs 2015-2019 climatology)
    - em_population_dim (population context, attached as of the same calendar year)

  Per row:
    - excess_deaths = deaths_total - expected_deaths.
    - excess_pct = excess_deaths / expected_deaths.
    - is_heat_week: heat_anomaly_c > +2 deg C (one standard threshold for a hot week).
    - heat_attributable_excess: excess_deaths counted only when is_heat_week,
      excluding negative excess (cool weeks bring no negative heat attribution).

  Attribution scope: heat is one driver of excess; cold and infectious disease are
  others. We label only the positive excess on hot weeks to avoid claiming heat
  attribution on otherwise-elevated weeks. This is conservative.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - eu_mortality_staging.em_mortality_panel
  - eu_mortality_staging.em_excess_baseline
  - eu_mortality_staging.em_temperature_panel
  - eu_mortality_staging.em_population_dim

tags:
  - eu-27
  - mortality
  - staging
  - panel
  - heat-attribution

columns:
  - name: nuts_id
    type: VARCHAR
    description: NUTS3 region code.
    primary_key: true
  - name: iso_year
    type: INTEGER
    description: ISO 8601 calendar year.
    primary_key: true
  - name: iso_week
    type: INTEGER
    description: ISO 8601 week number.
    primary_key: true
  - name: week_start_date
    type: DATE
    description: Monday of the ISO week.
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-2.
  - name: country_name_en
    type: VARCHAR
    description: English country name.
  - name: deaths_total
    type: DOUBLE
    description: Observed deaths in the week.
  - name: expected_deaths
    type: DOUBLE
    description: 2015-2019 climatological baseline.
  - name: excess_deaths
    type: DOUBLE
    description: deaths_total - expected_deaths.
  - name: excess_pct
    type: DOUBLE
    description: excess_deaths / expected_deaths.
  - name: temp_max_p95
    type: DOUBLE
    description: 95th-percentile weekly daily max temperature.
  - name: heat_anomaly_c
    type: DOUBLE
    description: Anomaly vs 2015-2019 climatology (deg C).
  - name: is_heat_week
    type: BOOLEAN
    description: heat_anomaly_c > 2 deg C.
  - name: heat_attributable_excess
    type: DOUBLE
    description: Positive excess attributable to heat. 0 outside heat weeks.
  - name: pop_total
    type: DOUBLE
    description: NUTS3 total population in the same calendar year.
  - name: pop_65plus
    type: DOUBLE
    description: NUTS3 population 65+ in the same calendar year.

@bruin */

WITH m AS (
    SELECT * FROM `bruin-playground-arsalan.eu_mortality_staging.em_mortality_panel`
),
b AS (
    SELECT * FROM `bruin-playground-arsalan.eu_mortality_staging.em_excess_baseline`
),
t AS (
    SELECT * FROM `bruin-playground-arsalan.eu_mortality_staging.em_temperature_panel`
),
p AS (
    SELECT * FROM `bruin-playground-arsalan.eu_mortality_staging.em_population_dim`
)

SELECT
    m.nuts_id,
    m.iso_year,
    m.iso_week,
    m.week_start_date,
    m.country_code,
    m.country_name_en,
    m.deaths_total,
    b.expected_deaths,
    m.deaths_total - b.expected_deaths AS excess_deaths,
    SAFE_DIVIDE(m.deaths_total - b.expected_deaths, b.expected_deaths) AS excess_pct,
    t.temp_max_p95,
    t.heat_anomaly_c,
    COALESCE(t.heat_anomaly_c > 2.0, FALSE) AS is_heat_week,
    CASE
        WHEN COALESCE(t.heat_anomaly_c, 0) > 2.0
             AND (m.deaths_total - b.expected_deaths) > 0
        THEN m.deaths_total - b.expected_deaths
        ELSE 0
    END AS heat_attributable_excess,
    p.pop_total,
    p.pop_65plus
FROM m
LEFT JOIN b ON b.nuts_id = m.nuts_id AND b.iso_week = m.iso_week
LEFT JOIN t ON t.nuts_id = m.nuts_id AND t.iso_year = m.iso_year AND t.iso_week = m.iso_week
LEFT JOIN p ON p.nuts_id = m.nuts_id AND p.ref_year = m.iso_year
ORDER BY m.nuts_id, m.iso_year, m.iso_week
