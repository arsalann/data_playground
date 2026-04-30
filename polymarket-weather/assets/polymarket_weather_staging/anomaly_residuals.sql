/* @bruin

name: polymarket_weather_staging.anomaly_residuals
type: bq.sql
description: |
  Hourly cross-station anomaly diagnostics for every Meteostat station in every
  investigation city (Paris, London, Seoul, Toronto). For every (city, station, ts_utc)
  the row carries:

    - the station's reading (`temp_c`)
    - the median, IQR and member count across the *other* stations in the SAME CITY at
      the same UTC hour (peers, excluding self)
    - the peer-median residual (station temperature minus peer median)
    - a robust z-score against peers (residual / 1.4826·IQR)
    - a station-internal rolling baseline over the prior 12 h excluding the current
      hour, plus an internal z-score
    - an `is_anomaly` flag combining the two

  The peer self-join is scoped within `city`, so London peers are not compared to
  Seoul peers and so on. Open-Meteo grid rows are not included as peers; the grid is
  available in temperature_hourly for separate reference comparisons.

  Core forensic dataset for the multi-city Polymarket × weather investigation. The
  Paris-CDG anomalies on 2026-04-06 and 2026-04-15 are still present here; new
  cities will surface their own outliers if any.
connection: bruin-playground-arsalan
tags:
  - forensic_analysis
  - weather_data
  - anomaly_detection
  - polymarket_investigation
  - multi_city
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
  - name: city
    type: VARCHAR
    description: City identifier from the manifest. Composite primary key with source_id and ts_utc.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Paris
          - London
          - Seoul
          - Toronto
  - name: source_id
    type: VARCHAR
    description: Meteostat station identifier (5-digit WMO or alphanumeric). Composite primary key with city and ts_utc.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: ts_utc
    type: TIMESTAMP
    description: Observation timestamp in UTC.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: role
    type: VARCHAR
    description: primary or peer
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - primary
          - peer
  - name: ts_local
    type: TIMESTAMP
    description: Same observation in the city's local time
  - name: local_date
    type: DATE
    description: Calendar date in city local time
  - name: local_hour
    type: INTEGER
    description: Hour of the day (0-23) in city local time
  - name: source_label
    type: VARCHAR
    description: Human-readable station name
  - name: temp_c
    type: DOUBLE
    description: Station temperature reading in degrees Celsius
  - name: peer_count
    type: INTEGER
    description: Number of peer stations in the same city contributing to the median at this timestamp.
    checks:
      - name: positive
  - name: peer_median
    type: DOUBLE
    description: Median temperature across peer stations in the same city at this timestamp (excluding self).
  - name: peer_p25
    type: DOUBLE
    description: 25th percentile of same-city peer station temperatures
  - name: peer_p75
    type: DOUBLE
    description: 75th percentile of same-city peer station temperatures
  - name: peer_iqr
    type: DOUBLE
    description: Inter-quartile range across same-city peer stations
  - name: peer_residual
    type: DOUBLE
    description: temp_c minus peer_median; positive if the station is warmer than peers
  - name: peer_z
    type: DOUBLE
    description: Robust z-score = peer_residual / (1.4826 * peer_iqr); null when peer_iqr is zero
  - name: rolling_mean
    type: DOUBLE
    description: Mean of the same station's temperature over the prior 12 hours, excluding the current hour
  - name: rolling_std
    type: DOUBLE
    description: Standard deviation of the same station's temperature over the prior 12 hours, excluding the current hour
    checks:
      - name: non_negative
  - name: internal_z
    type: DOUBLE
    description: (temp_c - rolling_mean) / rolling_std using the station's own prior-12h baseline
  - name: is_anomaly
    type: BOOLEAN
    description: True when ABS(peer_residual) >= 3 and ABS(peer_z) >= 2 (single-station outlier not supported by same-city peers)
  - name: anomaly_direction
    type: VARCHAR
    description: up when peer_residual > 0, down when < 0, NULL otherwise
    checks:
      - name: accepted_values
        value:
          - up
          - down

@bruin */

WITH stations_only AS (
    SELECT
        city,
        source_id,
        ts_utc,
        role,
        ts_local,
        local_date,
        local_hour,
        source_label,
        temp_c
    FROM `bruin-playground-arsalan.polymarket_weather_staging.temperature_hourly`
    WHERE source = 'meteostat' AND temp_c IS NOT NULL
),

self_x_peers AS (
    SELECT
        a.city,
        a.source_id,
        a.ts_utc,
        a.role,
        a.ts_local,
        a.local_date,
        a.local_hour,
        a.source_label,
        a.temp_c,
        ARRAY_AGG(b.temp_c IGNORE NULLS) AS peer_temps
    FROM stations_only a
    JOIN stations_only b
      ON a.city = b.city
     AND a.ts_utc = b.ts_utc
     AND a.source_id != b.source_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

with_peer_stats AS (
    SELECT
        city,
        source_id,
        ts_utc,
        role,
        ts_local,
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
            PARTITION BY city, source_id
            ORDER BY UNIX_SECONDS(ts_utc)
            RANGE BETWEEN 43200 PRECEDING AND 1 PRECEDING
        ) AS rolling_mean,
        STDDEV_SAMP(temp_c) OVER (
            PARTITION BY city, source_id
            ORDER BY UNIX_SECONDS(ts_utc)
            RANGE BETWEEN 43200 PRECEDING AND 1 PRECEDING
        ) AS rolling_std
    FROM with_residuals
)

SELECT
    city,
    source_id,
    ts_utc,
    role,
    ts_local,
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
