/* @bruin

name: polymarket_weather_staging.temperature_hourly
type: bq.sql
description: |
  Long-format hourly temperature panel covering every Meteostat station and
  Open-Meteo grid point listed in `city_manifest.yml` (Paris, London, Seoul,
  Toronto). Each row is one hourly observation from one source for one city.

  Cross-source comparisons must filter on `source` (meteostat vs openmeteo_grid)
  to avoid mixing station data with the grid reanalysis (different physical
  products). Cross-station anomaly detection (downstream) self-joins WITHIN a
  single `city`. Local time is computed once here per the city's IANA timezone
  so downstream assets do not have to repeat the conversion.

  Renamed local-time columns: `ts_local`, `local_date`, `local_hour` (formerly
  ts_local_paris, etc.). Paris-only filters are preserved via the `is_april_2026`
  flag (Europe/Paris timezone).
connection: bruin-playground-arsalan
tags:
  - sensor-tampering-investigation
  - weather-data
  - prediction-markets
  - multi-city
  - fact-table
  - hourly-observations
  - staging
  - cross-validation
  - meteostat-source
  - era5-reanalysis
  - polymarket-resolution

materialization:
  type: table
  strategy: create+replace

depends:
  - polymarket_weather_raw.station_hourly
  - polymarket_weather_raw.openmeteo_grid

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: city
    type: VARCHAR
    description: City identifier from the manifest. Composite primary key with source / source_id / ts_utc.
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
    description: Either 'meteostat' (METAR/SYNOP-fed station observation) or 'openmeteo_grid' (ERA5-based reanalysis at city centre)
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
    description: Meteostat station id for stations, '<city>_centre' for grid points
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: ts_utc
    type: TIMESTAMP
    description: Observation timestamp in UTC
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: role
    type: VARCHAR
    description: primary for the Polymarket-resolution station, peer for cross-station controls, grid for Open-Meteo reanalysis
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - primary
          - peer
          - grid
  - name: timezone
    type: VARCHAR
    description: IANA timezone string applied to derive local-time columns
    checks:
      - name: not_null
  - name: ts_local
    type: TIMESTAMP
    description: Same observation expressed in the city's local time. Automatically accounts for daylight saving transitions.
    checks:
      - name: not_null
  - name: local_date
    type: DATE
    description: Calendar date of the observation in city local time. Used for daily aggregations and event-window filters.
    checks:
      - name: not_null
  - name: local_hour
    type: INTEGER
    description: Hour of the day (0-23) in city local time.
    checks:
      - name: not_null
  - name: temp_c
    type: DOUBLE
    description: Air temperature at 2 metres above ground level in degrees Celsius.
    checks:
      - name: not_null
  - name: source_label
    type: VARCHAR
    description: Human-readable source identifier - station name for Meteostat sources or 'Open-Meteo grid (<city> centre)' for reanalysis.
    checks:
      - name: not_null
  - name: latitude
    type: DOUBLE
    description: Latitude of the observation point in decimal degrees (WGS84).
    checks:
      - name: not_null
  - name: longitude
    type: DOUBLE
    description: Longitude of the observation point in decimal degrees (WGS84).
    checks:
      - name: not_null
  - name: elevation_m
    type: DOUBLE
    description: Elevation above mean sea level in metres.
  - name: in_2026_window
    type: BOOLEAN
    description: True if local_date falls within 2026-01-01..2026-04-30 (the multi-city investigation window)
    checks:
      - name: not_null
  - name: is_april_2026
    type: BOOLEAN
    description: True if local_date falls within April 2026 (Paris-CDG forensic focus, Europe/Paris timezone)
    checks:
      - name: not_null

@bruin */

WITH city_meta AS (
    SELECT * FROM UNNEST([
        STRUCT('Paris'   AS city, 'Europe/Paris'   AS timezone),
        STRUCT('London'  AS city, 'Europe/London'  AS timezone),
        STRUCT('Seoul'   AS city, 'Asia/Seoul'     AS timezone),
        STRUCT('Toronto' AS city, 'America/Toronto' AS timezone)
    ])
),

station AS (
    SELECT
        city,
        'meteostat' AS source,
        station_id  AS source_id,
        ts_utc,
        role,
        station_name AS source_label,
        latitude,
        longitude,
        elevation_m,
        temp_c
    FROM `bruin-playground-arsalan.polymarket_weather_raw.station_hourly`
    WHERE temp_c IS NOT NULL
),

grid AS (
    SELECT
        city,
        'openmeteo_grid' AS source,
        CONCAT(LOWER(city), '_centre') AS source_id,
        ts_utc,
        'grid' AS role,
        CONCAT('Open-Meteo grid (', city, ' centre)') AS source_label,
        latitude,
        longitude,
        elevation_m,
        temp_c
    FROM `bruin-playground-arsalan.polymarket_weather_raw.openmeteo_grid`
    WHERE temp_c IS NOT NULL
),

unioned AS (
    SELECT * FROM station
    UNION ALL
    SELECT * FROM grid
)

SELECT
    u.city,
    u.source,
    u.source_id,
    u.ts_utc,
    u.role,
    cm.timezone,
    DATETIME(u.ts_utc, cm.timezone) AS ts_local,
    DATE(u.ts_utc, cm.timezone) AS local_date,
    EXTRACT(HOUR FROM DATETIME(u.ts_utc, cm.timezone)) AS local_hour,
    u.temp_c,
    u.source_label,
    u.latitude,
    u.longitude,
    u.elevation_m,
    DATE(u.ts_utc, cm.timezone) BETWEEN DATE '2026-01-01' AND DATE '2026-04-30' AS in_2026_window,
    u.city = 'Paris'
        AND DATE(u.ts_utc, 'Europe/Paris') BETWEEN DATE '2026-04-01' AND DATE '2026-04-30' AS is_april_2026
FROM unioned u
JOIN city_meta cm USING (city)
