/* @bruin

name: polymarket_weather_report.primary_station_anomalies
type: bq.sql
description: |
  Subset of cross-station anomalies where the Polymarket-resolution primary
  station was itself the outlier. These are the forensic targets - cases where
  the station Polymarket cites for resolution diverged sharply from same-city
  peers. Used by the dashboard's "primary station residual time series" chart
  and the per-city top-anomaly drill-in.
connection: bruin-playground-arsalan
tags:
  - report
  - forensics
  - anomaly_detection
  - multi_city
  - primary_station

materialization:
  type: table
  strategy: create+replace

depends:
  - polymarket_weather_staging.anomaly_residuals

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
  - name: ts_utc
    type: TIMESTAMP
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: source_id
    type: VARCHAR
  - name: source_label
    type: VARCHAR
  - name: ts_local
    type: TIMESTAMP
  - name: local_date
    type: DATE
  - name: local_hour
    type: INT64
  - name: temp_c
    type: FLOAT64
  - name: peer_median
    type: FLOAT64
  - name: peer_residual
    type: FLOAT64
  - name: peer_z
    type: FLOAT64
  - name: anomaly_direction
    type: VARCHAR

@bruin */

SELECT
    city,
    ts_utc,
    source_id,
    source_label,
    ts_local,
    local_date,
    local_hour,
    ROUND(temp_c, 2) AS temp_c,
    ROUND(peer_median, 2) AS peer_median,
    ROUND(peer_residual, 2) AS peer_residual,
    ROUND(peer_z, 2) AS peer_z,
    anomaly_direction
FROM `bruin-playground-arsalan.polymarket_weather_staging.anomaly_residuals`
WHERE role = 'primary'
  AND is_anomaly = TRUE
  AND local_date BETWEEN DATE '2026-01-01' AND DATE '2026-04-30'
ORDER BY city, ABS(peer_z) DESC
