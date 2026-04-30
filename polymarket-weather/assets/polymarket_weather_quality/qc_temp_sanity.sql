/* @bruin

name: polymarket_weather_quality.qc_temp_sanity
type: bq.sql
description: |
  Per-(city, station) count of hourly readings whose temp_c falls outside the
  physically plausible range [-30, 50] degrees Celsius. These rows almost always
  indicate sensor failure or unit-conversion errors, not weather signal. Any
  city/station with non-zero out_of_range_count should be reviewed before
  trusting the anomaly detection downstream.
connection: bruin-playground-arsalan
tags:
  - quality
  - sanity_check
  - multi_city

materialization:
  type: table
  strategy: create+replace

depends:
  - polymarket_weather_staging.temperature_hourly

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: city
    type: VARCHAR
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: source_id
    type: VARCHAR
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: source
    type: VARCHAR
  - name: rows_total
    type: INTEGER
  - name: out_of_range_count
    type: INTEGER
    description: Hours with temp_c outside [-30, 50]
  - name: extreme_min_c
    type: DOUBLE
  - name: extreme_max_c
    type: DOUBLE
  - name: status
    type: VARCHAR
    checks:
      - name: accepted_values
        value:
          - pass
          - warn
          - fail

@bruin */

WITH per_station AS (
    SELECT
        city,
        source,
        source_id,
        COUNT(*) AS rows_total,
        COUNTIF(temp_c < -30 OR temp_c > 50) AS out_of_range_count,
        MIN(temp_c) AS extreme_min_c,
        MAX(temp_c) AS extreme_max_c
    FROM `bruin-playground-arsalan.polymarket_weather_staging.temperature_hourly`
    WHERE local_date BETWEEN DATE '2026-01-01' AND DATE '2026-04-30'
    GROUP BY 1, 2, 3
)
SELECT
    city,
    source_id,
    source,
    rows_total,
    out_of_range_count,
    extreme_min_c,
    extreme_max_c,
    CASE
        WHEN out_of_range_count = 0 THEN 'pass'
        WHEN out_of_range_count <= 5 THEN 'warn'
        ELSE 'fail'
    END AS status
FROM per_station
