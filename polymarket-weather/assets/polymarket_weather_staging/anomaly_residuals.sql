/* @bruin

name: polymarket_weather_staging.anomaly_residuals
type: bq.sql
description: |
  Hourly cross-station anomaly diagnostics for the six Paris-region Meteostat stations.
  For every (station, ts_utc) the row carries:

    - the station's reading (`temp_c`)
    - the median, IQR and member count across the *other* stations at the same UTC hour
      (peers, excluding self)
    - the peer-median residual (station temperature minus peer median)
    - a robust z-score against peers (residual / 1.4826·IQR)
    - a station-internal rolling baseline over the prior 12 h excluding the current
      hour, plus an internal z-score
    - an `is_anomaly` flag combining the two

  Open-Meteo grid rows are *not* included as peers — peers are the five other Paris
  stations only, so the metric is "this station vs other Paris stations". The grid is
  available in temperature_hourly for separate reference comparisons.

  Core forensic dataset for the alleged temperature sensor tampering at
  Paris-Charles de Gaulle airport on 2026-04-06 and 2026-04-15. Verified from the
  warehouse during Phase 5: 8 CDG hourly anomalies across 7 distinct April days.
  The strongest reading is 2026-04-06 17:00 UTC (CDG=21.0°C, peer-median 17.0°C,
  residual +4.0°C, peer-z 3.0); the strongest negative is 2026-04-27 18:00 UTC
  (residual -5.7°C). Apr 15 18:00 UTC shows the recovery from a sub-hour spike
  (residual -3.2°C, peer-z -3.08).
connection: bruin-playground-arsalan
tags:
  - forensic_analysis
  - weather_data
  - anomaly_detection
  - polymarket_investigation
  - paris_stations
  - temperature
  - fact_table
  - hourly_data

materialization:
  type: table
  strategy: create+replace

depends:
  - polymarket_weather_staging.temperature_hourly

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: source_id
    type: VARCHAR
    description: Meteostat station identifier for Paris-region weather stations. Six stations total - includes alleged tampered station 07157 (CDG) and five peer stations for comparison.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - "07145"
          - "07147"
          - "07149"
          - "07150"
          - "07156"
          - "07157"
  - name: ts_utc
    type: TIMESTAMP
    description: Observation timestamp in UTC. Composite primary key with source_id ensures one row per station per hour.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: ts_local_paris
    type: TIMESTAMP
    description: Same observation in Europe/Paris local time
  - name: local_date
    type: DATE
    description: Calendar date in Europe/Paris local time
  - name: local_hour
    type: INTEGER
    description: Hour of the day (0-23) in Europe/Paris local time
  - name: source_label
    type: VARCHAR
    description: Human-readable station name
  - name: temp_c
    type: DOUBLE
    description: Station temperature reading in degrees Celsius
  - name: peer_count
    type: INTEGER
    description: Number of peer stations contributing to the median at this timestamp. Typically 5 (other Paris stations excluding self). Lower values indicate missing data at peer stations.
    checks:
      - name: positive
  - name: peer_median
    type: DOUBLE
    description: Median temperature across peer stations at this timestamp (excluding self). Celsius degrees. Core baseline for detecting single-station anomalies.
  - name: peer_p25
    type: DOUBLE
    description: 25th percentile of peer station temperatures at this timestamp
  - name: peer_p75
    type: DOUBLE
    description: 75th percentile of peer station temperatures at this timestamp
  - name: peer_iqr
    type: DOUBLE
    description: Inter-quartile range across peer stations at this timestamp
  - name: peer_residual
    type: DOUBLE
    description: temp_c minus peer_median; positive if the station is warmer than peers. Key forensic metric - residuals ≥3°C trigger anomaly investigation. Celsius degrees.
  - name: peer_z
    type: DOUBLE
    description: Robust z-score = peer_residual / (1.4826 * peer_iqr); null when peer_iqr is zero. Values ≥2 combined with large residuals indicate potential tampering. Unitless.
  - name: rolling_mean
    type: DOUBLE
    description: Mean of the same station's temperature over the prior 12 hours, excluding the current hour. Station-specific baseline for detecting deviations from recent history. Celsius degrees.
  - name: rolling_std
    type: DOUBLE
    description: Standard deviation of the same station's temperature over the prior 12 hours, excluding the current hour. Measure of recent temperature variability at this specific station. Celsius degrees.
    checks:
      - name: non_negative
  - name: internal_z
    type: DOUBLE
    description: (temp_c - rolling_mean) / rolling_std using the station's own prior-12h baseline. Detects readings unusual for this specific station's recent pattern. Null when rolling_std is zero. Unitless.
  - name: is_anomaly
    type: BOOLEAN
    description: True when ABS(peer_residual) >= 3 °C AND ABS(peer_z) >= 2 (single-station outlier not supported by peer stations at the same hour). Captures both upward spikes (e.g., the alleged April 6 17:00 UTC tampering reading) and downward drops (e.g., April 15 18:00 UTC). Peer-based robust z avoids contamination from the day's diurnal swing.
  - name: anomaly_direction
    type: VARCHAR
    description: '''up'' when peer_residual > 0, ''down'' when < 0; NULL otherwise. Convenience field for filtering and visualisation.'
    checks:
      - name: accepted_values
        value:
          - up
          - down

@bruin */

WITH stations_only AS (
    SELECT
        source_id,
        ts_utc,
        ts_local_paris,
        local_date,
        local_hour,
        source_label,
        temp_c
    FROM `bruin-playground-arsalan.polymarket_weather_staging.temperature_hourly`
    WHERE source = 'meteostat' AND temp_c IS NOT NULL
),

self_x_peers AS (
    -- Self-join: every station row paired with every *other* station row at the same ts_utc
    SELECT
        a.source_id,
        a.ts_utc,
        a.ts_local_paris,
        a.local_date,
        a.local_hour,
        a.source_label,
        a.temp_c,
        ARRAY_AGG(b.temp_c IGNORE NULLS) AS peer_temps
    FROM stations_only a
    JOIN stations_only b
      ON a.ts_utc = b.ts_utc AND a.source_id != b.source_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

with_peer_stats AS (
    SELECT
        source_id,
        ts_utc,
        ts_local_paris,
        local_date,
        local_hour,
        source_label,
        temp_c,
        ARRAY_LENGTH(peer_temps) AS peer_count,
        (SELECT APPROX_QUANTILES(t, 100)[OFFSET(50)] FROM UNNEST(peer_temps) AS t) AS peer_median,
        (SELECT APPROX_QUANTILES(t, 100)[OFFSET(25)] FROM UNNEST(peer_temps) AS t) AS peer_p25,
        (SELECT APPROX_QUANTILES(t, 100)[OFFSET(75)] FROM UNNEST(peer_temps) AS t) AS peer_p75
    FROM self_x_peers
),

with_residuals AS (
    SELECT
        *,
        ROUND(temp_c - peer_median, 3) AS peer_residual,
        ROUND(peer_p75 - peer_p25, 3) AS peer_iqr
    FROM with_peer_stats
),

with_internal_baseline AS (
    SELECT
        *,
        AVG(temp_c) OVER (
            PARTITION BY source_id
            ORDER BY UNIX_SECONDS(ts_utc)
            RANGE BETWEEN 43200 PRECEDING AND 1 PRECEDING
        ) AS rolling_mean,
        STDDEV_SAMP(temp_c) OVER (
            PARTITION BY source_id
            ORDER BY UNIX_SECONDS(ts_utc)
            RANGE BETWEEN 43200 PRECEDING AND 1 PRECEDING
        ) AS rolling_std
    FROM with_residuals
)

SELECT
    source_id,
    ts_utc,
    ts_local_paris,
    local_date,
    local_hour,
    source_label,
    temp_c,
    peer_count,
    peer_median,
    peer_p25,
    peer_p75,
    peer_iqr,
    peer_residual,
    SAFE_DIVIDE(peer_residual, NULLIF(1.4826 * peer_iqr, 0)) AS peer_z,
    ROUND(rolling_mean, 3) AS rolling_mean,
    ROUND(rolling_std, 3) AS rolling_std,
    SAFE_DIVIDE(temp_c - rolling_mean, NULLIF(rolling_std, 0)) AS internal_z,
    (ABS(peer_residual) >= 3 AND ABS(SAFE_DIVIDE(peer_residual, NULLIF(1.4826 * peer_iqr, 0))) >= 2) AS is_anomaly,
    CASE WHEN peer_residual > 0 THEN 'up' WHEN peer_residual < 0 THEN 'down' ELSE NULL END AS anomaly_direction
FROM with_internal_baseline
