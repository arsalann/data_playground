/* @bruin

name: polymarket_weather_staging.temperature_daily
type: bq.sql
description: |
  Daily temperature panel for every Meteostat station and Open-Meteo grid point
  in city_manifest.yml (Paris, London, Seoul, Toronto). Used both for
  Polymarket-style daily-max resolution counterfactuals and for multi-year
  climatology context.

  Two source paths are unioned with preference logic:
    1. Pre-aggregated daily series from raw.station_daily (long history baseline)
    2. Daily max/min/mean derived from hourly observations (raw.station_hourly +
       raw.openmeteo_grid) — the *Polymarket-equivalent* daily max, computed from
       local-day hourly readings using each city's IANA timezone.

  When both sources exist for the same (city, station, date), the hourly-derived
  row is preferred because it precisely matches what Polymarket's resolution
  logic would observe from real-time feeds.
connection: bruin-playground-arsalan
tags:
  - domain:forensic_investigation
  - domain:finance
  - data_type:fact_table
  - data_type:time_series
  - source:meteostat
  - source:openmeteo
  - geography:multi_city
  - temporal_scope:climatology
  - pipeline_role:staging
  - update_pattern:snapshot
  - sensitivity:public

materialization:
  type: table
  strategy: create+replace

depends:
  - polymarket_weather_raw.station_daily
  - polymarket_weather_raw.station_hourly
  - polymarket_weather_raw.openmeteo_grid

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: city
    type: VARCHAR
    description: City identifier from the manifest
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
  - name: source
    type: VARCHAR
    description: meteostat (station observation) or openmeteo_grid (ERA5 reanalysis)
    primary_key: true
    nullable: false
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - meteostat
          - openmeteo_grid
  - name: source_id
    type: VARCHAR
    description: Meteostat station identifier or '<city>_centre' for the grid
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: local_date
    type: DATE
    description: Calendar date in the city's local timezone
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: role
    type: VARCHAR
    description: primary, peer, or grid
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - primary
          - peer
          - grid
  - name: source_label
    type: VARCHAR
    description: Human-readable station name
    nullable: false
    checks:
      - name: not_null
  - name: temp_max_c
    type: DOUBLE
    description: Daily maximum temperature in degrees Celsius
  - name: temp_min_c
    type: DOUBLE
    description: Daily minimum temperature in degrees Celsius
  - name: temp_mean_c
    type: DOUBLE
    description: Daily mean temperature in degrees Celsius
  - name: temp_max_bucket_c
    type: INTEGER
    description: Daily maximum rounded to whole degrees Celsius - the bucket Polymarket uses to resolve daily-max temperature markets
  - name: derivation
    type: VARCHAR
    description: hourly_derived or station_daily
    nullable: false
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - hourly_derived
          - station_daily

@bruin */

WITH city_meta AS (
    SELECT * FROM UNNEST([
        STRUCT('Paris'   AS city, 'Europe/Paris'   AS timezone),
        STRUCT('London'  AS city, 'Europe/London'  AS timezone),
        STRUCT('Seoul'   AS city, 'Asia/Seoul'     AS timezone),
        STRUCT('Toronto' AS city, 'America/Toronto' AS timezone)
    ])
),

station_daily_dedup AS (
    SELECT * EXCEPT(rn) FROM (
        SELECT
            city, station_id, date, role, station_name,
            temp_max_c, temp_min_c, temp_mean_c,
            ROW_NUMBER() OVER (PARTITION BY city, station_id, date ORDER BY extracted_at DESC) AS rn
        FROM `bruin-playground-arsalan.polymarket_weather_raw.station_daily`
    )
    WHERE rn = 1
),

hourly_station AS (
    SELECT
        h.city,
        'meteostat' AS source,
        h.station_id  AS source_id,
        DATE(h.ts_utc, cm.timezone) AS local_date,
        ANY_VALUE(h.role) AS role,
        ANY_VALUE(h.station_name) AS source_label,
        MAX(h.temp_c) AS temp_max_c,
        MIN(h.temp_c) AS temp_min_c,
        AVG(h.temp_c) AS temp_mean_c
    FROM `bruin-playground-arsalan.polymarket_weather_raw.station_hourly` h
    JOIN city_meta cm USING (city)
    WHERE h.temp_c IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

hourly_grid AS (
    SELECT
        g.city,
        'openmeteo_grid' AS source,
        CONCAT(LOWER(g.city), '_centre') AS source_id,
        DATE(g.ts_utc, cm.timezone) AS local_date,
        'grid' AS role,
        CONCAT('Open-Meteo grid (', g.city, ' centre)') AS source_label,
        MAX(g.temp_c) AS temp_max_c,
        MIN(g.temp_c) AS temp_min_c,
        AVG(g.temp_c) AS temp_mean_c
    FROM `bruin-playground-arsalan.polymarket_weather_raw.openmeteo_grid` g
    JOIN city_meta cm USING (city)
    WHERE g.temp_c IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

prefer_hourly AS (
    SELECT * FROM hourly_station
    UNION ALL
    SELECT * FROM hourly_grid
),

station_daily_only AS (
    SELECT
        sd.city,
        'meteostat' AS source,
        sd.station_id AS source_id,
        sd.date AS local_date,
        sd.role,
        sd.station_name AS source_label,
        sd.temp_max_c,
        sd.temp_min_c,
        sd.temp_mean_c
    FROM station_daily_dedup sd
    LEFT JOIN hourly_station h
        ON sd.city = h.city AND sd.station_id = h.source_id AND sd.date = h.local_date
    WHERE h.local_date IS NULL
),

unioned AS (
    SELECT *, 'hourly_derived' AS derivation FROM prefer_hourly
    UNION ALL
    SELECT *, 'station_daily' AS derivation FROM station_daily_only
)

SELECT
    city,
    source,
    source_id,
    local_date,
    role,
    source_label,
    ROUND(temp_max_c, 2) AS temp_max_c,
    ROUND(temp_min_c, 2) AS temp_min_c,
    ROUND(temp_mean_c, 2) AS temp_mean_c,
    CAST(ROUND(temp_max_c) AS INT64) AS temp_max_bucket_c,
    derivation
FROM unioned
