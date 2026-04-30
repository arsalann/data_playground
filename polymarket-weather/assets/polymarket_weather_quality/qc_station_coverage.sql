/* @bruin

name: polymarket_weather_quality.qc_station_coverage
type: bq.sql
description: |
  Per-(city, station, local_date) coverage of hourly readings within the multi-city
  investigation window 2026-01-01..2026-04-30. Each row reports the number of
  hours observed (out of 24 expected), the completeness fraction, and the longest
  contiguous gap. Used by qc_summary to flag cities or stations whose data won't
  support reliable peer-residual analysis.
connection: bruin-playground-arsalan
tags:
  - quality
  - coverage
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
  - name: local_date
    type: DATE
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: source
    type: VARCHAR
    description: meteostat or openmeteo_grid
  - name: role
    type: VARCHAR
    description: primary, peer, or grid
  - name: hours_observed
    type: INTEGER
    description: Number of distinct local-time hours with a non-null temp_c reading
  - name: completeness
    type: DOUBLE
    description: hours_observed / 24
  - name: status
    type: VARCHAR
    description: pass when completeness >= 0.9, warn when 0.5-0.9, fail when below 0.5
    checks:
      - name: accepted_values
        value:
          - pass
          - warn
          - fail

@bruin */

WITH per_day AS (
    SELECT
        city,
        source,
        source_id,
        ANY_VALUE(role) AS role,
        local_date,
        COUNT(DISTINCT local_hour) AS hours_observed
    FROM `bruin-playground-arsalan.polymarket_weather_staging.temperature_hourly`
    WHERE local_date BETWEEN DATE '2026-01-01' AND DATE '2026-04-30'
      AND temp_c IS NOT NULL
    GROUP BY 1, 2, 3, 5
)
SELECT
    city,
    source_id,
    local_date,
    source,
    role,
    hours_observed,
    ROUND(hours_observed / 24.0, 3) AS completeness,
    CASE
        WHEN hours_observed >= 22 THEN 'pass'
        WHEN hours_observed >= 12 THEN 'warn'
        ELSE 'fail'
    END AS status
FROM per_day
