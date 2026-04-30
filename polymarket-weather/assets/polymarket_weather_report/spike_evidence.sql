/* @bruin

name: polymarket_weather_report.spike_evidence
type: bq.sql
description: |
  Joined hourly station temperatures and Polymarket bucket prices for the two
  alleged-tampering days (2026-04-06 and 2026-04-15) plus one day on either side.
  One row per (event_local_date, source, source_id, ts_utc) for the temperature
  side; the price side is delivered separately under the same time grain.

  This is the dashboard's section-1 data feed. The dashboard will overlay the CDG
  hourly temperature trace against the price tick chart for the winning bucket of
  each event so the timing of the price jump can be visually aligned with the
  hourly anomaly.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - polymarket_weather_staging.temperature_hourly
  - polymarket_weather_staging.anomaly_residuals

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: event_local_date
    type: DATE
    description: Anchor event date (2026-04-06 or 2026-04-15)
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: source
    type: VARCHAR
    description: meteostat / openmeteo_grid
    primary_key: true
    nullable: false
  - name: source_id
    type: VARCHAR
    description: Station id or paris_centre
    primary_key: true
    nullable: false
  - name: ts_utc
    type: TIMESTAMP
    description: Hourly observation timestamp (UTC)
    primary_key: true
    nullable: false
  - name: ts_local_paris
    type: TIMESTAMP
    description: Same timestamp in Europe/Paris local time
  - name: source_label
    type: VARCHAR
    description: Human-readable station / source label
  - name: temp_c
    type: DOUBLE
    description: Hourly temperature in degrees Celsius
  - name: peer_residual
    type: DOUBLE
    description: Station temp_c minus peer median for the same UTC hour (NULL for the Open-Meteo grid)
  - name: peer_z
    type: DOUBLE
    description: Robust peer-based z-score (NULL for the Open-Meteo grid)
  - name: is_anomaly
    type: BOOLEAN
    description: Whether this row was flagged as a single-station anomaly in anomaly_residuals
  - name: is_cdg
    type: BOOLEAN
    description: Whether this row is the suspect Paris-CDG station

@bruin */

WITH window_dates AS (
    SELECT event_local_date FROM UNNEST([
        DATE '2026-04-05', DATE '2026-04-06', DATE '2026-04-07',
        DATE '2026-04-14', DATE '2026-04-15', DATE '2026-04-16'
    ]) AS event_local_date
)

SELECT
    CASE WHEN th.local_date IN (DATE '2026-04-05', DATE '2026-04-06', DATE '2026-04-07')
         THEN DATE '2026-04-06'
         ELSE DATE '2026-04-15' END AS event_local_date,
    th.source,
    th.source_id,
    th.ts_utc,
    CAST(th.ts_local AS TIMESTAMP) AS ts_local_paris,
    th.source_label,
    th.temp_c,
    ar.peer_residual,
    ar.peer_z,
    COALESCE(ar.is_anomaly, FALSE) AS is_anomaly,
    th.source_id = '07157' AND th.source = 'meteostat' AS is_cdg
FROM `bruin-playground-arsalan.polymarket_weather_staging.temperature_hourly` th
LEFT JOIN `bruin-playground-arsalan.polymarket_weather_staging.anomaly_residuals` ar
    ON ar.city = th.city AND ar.source_id = th.source_id AND ar.ts_utc = th.ts_utc
WHERE th.city = 'Paris'
  AND th.local_date IN (
    DATE '2026-04-05', DATE '2026-04-06', DATE '2026-04-07',
    DATE '2026-04-14', DATE '2026-04-15', DATE '2026-04-16'
)
