/* @bruin

name: eu_mortality_staging.em_temperature_panel
type: bq.sql
description: |
  Weekly NUTS3 temperature panel built by aggregating daily Open-Meteo
  ERA5-Seamless reanalysis to ISO weeks.

  Per (nuts_id, iso_year, iso_week):
    - temp_max_p95: 95th percentile of daily max temperatures during the week
      (captures the hottest day in the week, the heat-attribution-relevant signal).
    - temp_max_mean: mean of daily max temperatures.
    - temp_mean_mean: mean of daily mean temperatures.
    - n_days: number of days with data (sanity check; should be 7 for complete weeks).

  Plus heat-anomaly columns relative to a 2015-2019 climatology of the same ISO
  week-number for the same NUTS3 region:
    - temp_max_p95_climo: 2015-2019 mean of temp_max_p95 for this week_of_year.
    - heat_anomaly_c: temp_max_p95 - temp_max_p95_climo.

  The 2015-2019 baseline is the standard ISGlobal / Ballester (2023) framing for
  excess-mortality heat attribution and predates the abnormally hot 2022 / 2024 /
  2025 summers.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - eu_mortality_raw.openmeteo_daily_temperature
  - eu_mortality_staging.em_nuts3_dim

tags:
  - eu-27
  - mortality
  - staging
  - temperature
  - climatology

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
  - name: temp_max_p95
    type: DOUBLE
    description: 95th percentile of daily max temperature (degrees C) within the week.
  - name: temp_max_mean
    type: DOUBLE
    description: Mean daily max temperature within the week (degrees C).
  - name: temp_mean_mean
    type: DOUBLE
    description: Mean daily mean temperature within the week (degrees C).
  - name: n_days
    type: INTEGER
    description: Days with data in the week (1-7).
  - name: temp_max_p95_climo
    type: DOUBLE
    description: 2015-2019 mean of temp_max_p95 for the same NUTS3 and week-of-year.
  - name: heat_anomaly_c
    type: DOUBLE
    description: temp_max_p95 - temp_max_p95_climo, degrees C.

@bruin */

WITH daily AS (
    SELECT
        t.nuts_id,
        t.obs_date,
        t.temp_max_c,
        t.temp_mean_c,
        EXTRACT(ISOYEAR FROM t.obs_date) AS iso_year,
        EXTRACT(ISOWEEK FROM t.obs_date) AS iso_week
    FROM `bruin-playground-arsalan.eu_mortality_raw.openmeteo_daily_temperature` t
    JOIN `bruin-playground-arsalan.eu_mortality_staging.em_nuts3_dim` d USING (nuts_id)
    WHERE t.temp_max_c IS NOT NULL
),

weekly AS (
    SELECT
        nuts_id,
        iso_year,
        iso_week,
        DATE_TRUNC(MIN(obs_date), WEEK(MONDAY)) AS week_start_date,
        APPROX_QUANTILES(temp_max_c, 100)[OFFSET(95)] AS temp_max_p95,
        AVG(temp_max_c) AS temp_max_mean,
        AVG(temp_mean_c) AS temp_mean_mean,
        COUNT(*) AS n_days
    FROM daily
    GROUP BY nuts_id, iso_year, iso_week
),

climo AS (
    SELECT
        nuts_id,
        iso_week,
        AVG(temp_max_p95) AS temp_max_p95_climo
    FROM weekly
    WHERE iso_year BETWEEN 2015 AND 2019
    GROUP BY nuts_id, iso_week
)

SELECT
    w.nuts_id,
    w.iso_year,
    w.iso_week,
    w.week_start_date,
    w.temp_max_p95,
    w.temp_max_mean,
    w.temp_mean_mean,
    CAST(w.n_days AS INT64) AS n_days,
    c.temp_max_p95_climo,
    w.temp_max_p95 - c.temp_max_p95_climo AS heat_anomaly_c
FROM weekly w
LEFT JOIN climo c USING (nuts_id, iso_week)
ORDER BY w.nuts_id, w.iso_year, w.iso_week
