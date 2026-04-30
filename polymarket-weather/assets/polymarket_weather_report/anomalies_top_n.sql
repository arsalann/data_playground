/* @bruin

name: polymarket_weather_report.anomalies_top_n
type: bq.sql
description: |
  Top-20 hourly cross-station anomalies per city across the multi-city
  investigation window 2026-01-01..2026-04-30, ordered by absolute peer-z.
  Each row identifies which station was the outlier, by how much, and which
  direction (up/down). The is_primary flag marks anomalies where the
  Polymarket-resolution station was itself the outlier - those are the
  forensic targets for the dashboard.
connection: bruin-playground-arsalan
tags:
  - report
  - forensics
  - anomaly_detection
  - multi_city

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
  - name: rank
    type: INT64
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: source_id
    type: VARCHAR
  - name: source_label
    type: VARCHAR
  - name: role
    type: VARCHAR
  - name: is_primary
    type: BOOLEAN
  - name: ts_utc
    type: TIMESTAMP
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

WITH ranked AS (
    SELECT
        city,
        source_id,
        source_label,
        role,
        role = 'primary' AS is_primary,
        ts_utc,
        ts_local,
        local_date,
        local_hour,
        temp_c,
        peer_median,
        peer_residual,
        peer_z,
        anomaly_direction,
        ROW_NUMBER() OVER (
            PARTITION BY city
            ORDER BY ABS(peer_z) DESC NULLS LAST
        ) AS rank
    FROM `bruin-playground-arsalan.polymarket_weather_staging.anomaly_residuals`
    WHERE local_date BETWEEN DATE '2026-01-01' AND DATE '2026-04-30'
      AND is_anomaly = TRUE
)
SELECT
    city,
    rank,
    source_id,
    source_label,
    role,
    is_primary,
    ts_utc,
    ts_local,
    local_date,
    local_hour,
    ROUND(temp_c, 2) AS temp_c,
    ROUND(peer_median, 2) AS peer_median,
    ROUND(peer_residual, 2) AS peer_residual,
    ROUND(peer_z, 2) AS peer_z,
    anomaly_direction
FROM ranked
WHERE rank <= 20
ORDER BY city, rank
