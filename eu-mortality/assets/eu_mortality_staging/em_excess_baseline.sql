/* @bruin

name: eu_mortality_staging.em_excess_baseline
type: bq.sql
description: |
  2015-2019 climatological baseline of expected weekly deaths per NUTS3 region.

  For each (nuts_id, iso_week), takes the simple mean of observed weekly deaths
  across 2015-2019 (five reference years that predate the abnormally hot 2022 /
  2024 / 2025 summers and the COVID-19 mortality shocks of 2020-2022).

  The ISGlobal / Ballester (2023) excess-mortality framework uses pre-pandemic
  baselines for the same reason: COVID-19 deaths bias 2020-2022 baselines upward
  and would attribute non-heat excess to heat.

  This is a deliberately simple baseline; richer alternatives (Serfling, ARIMA,
  GAM with smooth seasonal terms) are tracked as future-work in the dashboard
  methodology section.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - eu_mortality_staging.em_mortality_panel

tags:
  - eu-27
  - mortality
  - staging
  - baseline
  - climatology

columns:
  - name: nuts_id
    type: VARCHAR
    description: NUTS3 region code.
    primary_key: true
  - name: iso_week
    type: INTEGER
    description: ISO 8601 week number (1-53).
    primary_key: true
  - name: expected_deaths
    type: DOUBLE
    description: Mean weekly deaths for this NUTS3 + week-of-year across 2015-2019.
  - name: expected_deaths_sd
    type: DOUBLE
    description: Standard deviation of weekly deaths across 2015-2019 baseline years.
  - name: baseline_n_years
    type: INTEGER
    description: Number of years contributing to the baseline (1-5).

@bruin */

SELECT
    nuts_id,
    iso_week,
    AVG(deaths_total) AS expected_deaths,
    STDDEV_SAMP(deaths_total) AS expected_deaths_sd,
    COUNT(DISTINCT iso_year) AS baseline_n_years
FROM `bruin-playground-arsalan.eu_mortality_staging.em_mortality_panel`
WHERE iso_year BETWEEN 2015 AND 2019
GROUP BY nuts_id, iso_week
HAVING COUNT(DISTINCT iso_year) >= 3
ORDER BY nuts_id, iso_week
