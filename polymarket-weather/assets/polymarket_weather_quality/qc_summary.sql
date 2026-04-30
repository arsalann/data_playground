/* @bruin

name: polymarket_weather_quality.qc_summary
type: bq.sql
description: |
  Per-city QC roll-up combining station coverage, temperature sanity, price
  coverage, and resolution-mapping checks. Each row gives a single-glance
  health view per city. A city is `fail` if any sub-check fails, `warn` if
  any warns, `pass` only when all sub-checks pass.
connection: bruin-playground-arsalan
tags:
  - quality
  - summary
  - multi_city

materialization:
  type: table
  strategy: create+replace

depends:
  - polymarket_weather_quality.qc_station_coverage
  - polymarket_weather_quality.qc_temp_sanity
  - polymarket_weather_quality.qc_price_coverage
  - polymarket_weather_quality.qc_resolution_mapping

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
      - name: unique
  - name: station_coverage_status
    type: VARCHAR
  - name: stations_failing_coverage
    type: INTEGER
  - name: temp_sanity_status
    type: VARCHAR
  - name: stations_failing_sanity
    type: INTEGER
  - name: price_coverage_status
    type: VARCHAR
  - name: median_price_coverage
    type: DOUBLE
  - name: resolution_mapping_status
    type: VARCHAR
  - name: overall_status
    type: VARCHAR
    checks:
      - name: accepted_values
        value:
          - pass
          - warn
          - fail

@bruin */

WITH coverage_per_city AS (
    -- "fail" only when sparse-data days exceed 10% of the window; otherwise warn or pass.
    -- Isolated bad days are normal Meteostat sparsity and shouldn't block analysis.
    SELECT
        city,
        COUNTIF(status = 'fail') AS stations_failing_coverage,
        CASE
            WHEN SAFE_DIVIDE(COUNTIF(status = 'fail'), COUNT(*)) > 0.10 THEN 'fail'
            WHEN COUNTIF(status = 'fail') > 0 OR COUNTIF(status = 'warn') > 0 THEN 'warn'
            ELSE 'pass'
        END AS station_coverage_status
    FROM `bruin-playground-arsalan.polymarket_weather_quality.qc_station_coverage`
    GROUP BY city
),

sanity_per_city AS (
    SELECT
        city,
        COUNTIF(status = 'fail') AS stations_failing_sanity,
        CASE
            WHEN COUNTIF(status = 'fail') > 0 THEN 'fail'
            WHEN COUNTIF(status = 'warn') > 0 THEN 'warn'
            ELSE 'pass'
        END AS temp_sanity_status
    FROM `bruin-playground-arsalan.polymarket_weather_quality.qc_temp_sanity`
    GROUP BY city
),

price_per_city AS (
    SELECT
        city,
        status AS price_coverage_status,
        median_hourly_coverage AS median_price_coverage
    FROM `bruin-playground-arsalan.polymarket_weather_quality.qc_price_coverage`
),

resolution_per_city AS (
    SELECT
        city,
        status AS resolution_mapping_status
    FROM `bruin-playground-arsalan.polymarket_weather_quality.qc_resolution_mapping`
)

SELECT
    cov.city,
    cov.station_coverage_status,
    cov.stations_failing_coverage,
    san.temp_sanity_status,
    san.stations_failing_sanity,
    pri.price_coverage_status,
    pri.median_price_coverage,
    res.resolution_mapping_status,
    CASE
        WHEN 'fail' IN (cov.station_coverage_status, san.temp_sanity_status, pri.price_coverage_status, res.resolution_mapping_status) THEN 'fail'
        WHEN 'warn' IN (cov.station_coverage_status, san.temp_sanity_status, pri.price_coverage_status, res.resolution_mapping_status) THEN 'warn'
        ELSE 'pass'
    END AS overall_status
FROM coverage_per_city cov
LEFT JOIN sanity_per_city san USING (city)
LEFT JOIN price_per_city pri USING (city)
LEFT JOIN resolution_per_city res USING (city)
ORDER BY cov.city
