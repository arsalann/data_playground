/* @bruin

name: polymarket_weather_staging.temperature_hourly
type: bq.sql
description: |
  Long-format hourly temperature panel covering both the six Paris-region weather
  stations and the Open-Meteo gridded reanalysis at Paris centre.

  Each row is one hourly observation from one source. Every cross-source comparison
  in downstream staging and reports must filter on the `source` column to avoid mixing
  station data with the grid reanalysis (they are different physical products). The
  `temp_c` column is the only metric carried forward; auxiliary fields (humidity,
  pressure, etc.) are intentionally excluded from this canonical panel.

  Local Paris time is computed once here so downstream assets do not have to repeat
  the timezone conversion. April 2026 is flagged for fast filtering.
connection: bruin-playground-arsalan
tags:
  - sensor-tampering-investigation
  - weather-data
  - prediction-markets
  - paris-region
  - fact-table
  - hourly-observations
  - staging
  - cross-validation
  - meteostat-source
  - era5-reanalysis
  - polymarket-resolution
  - april-2026-investigation

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
  - name: source
    type: VARCHAR
    description: Either 'meteostat' (METAR/SYNOP-fed station observation) or 'openmeteo_grid' (ERA5-based reanalysis at Paris centre)
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
    description: Meteostat station id for stations, 'paris_centre' for the grid
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
  - name: ts_local_paris
    type: TIMESTAMP
    description: Same observation expressed in Europe/Paris local time (CET/CEST). Automatically accounts for daylight saving time transitions.
    checks:
      - name: not_null
  - name: local_date
    type: DATE
    description: Calendar date of the observation in Europe/Paris local time. Used for daily aggregations and April 2026 filtering.
    checks:
      - name: not_null
  - name: local_hour
    type: INTEGER
    description: Hour of the day (0-23) in Europe/Paris local time. Extracted from ts_local_paris for time-of-day analysis and filtering.
    checks:
      - name: not_null
  - name: temp_c
    type: DOUBLE
    description: Air temperature at 2 metres above ground level in degrees Celsius. Historical range -8°C to 39°C reflects Paris region climate with seasonal variation.
    checks:
      - name: not_null
  - name: source_label
    type: VARCHAR
    description: Human-readable source identifier. Station names for Meteostat sources (e.g., 'Paris-Charles de Gaulle', 'Trappes') or 'Open-Meteo grid (Paris centre)' for reanalysis data.
    checks:
      - name: not_null
  - name: latitude
    type: DOUBLE
    description: Latitude of the observation point in decimal degrees (WGS84). All stations within Paris metropolitan region (~48.7-49.0°N).
    checks:
      - name: not_null
  - name: longitude
    type: DOUBLE
    description: Longitude of the observation point in decimal degrees (WGS84). All stations within Paris metropolitan region (~2.0-2.6°E).
    checks:
      - name: not_null
  - name: elevation_m
    type: DOUBLE
    description: Elevation above mean sea level in metres. Paris region stations typically 50-180m elevation reflecting local topography.
    checks:
      - name: not_null
  - name: is_april_2026
    type: BOOLEAN
    description: Boolean flag indicating if local_date falls within April 2026 (2026-04-01 to 2026-04-30). Critical filtering field for isolating the sensor tampering investigation period.
    checks:
      - name: not_null

@bruin */

WITH station AS (
    SELECT
        'meteostat' AS source,
        station_id  AS source_id,
        ts_utc,
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
        'openmeteo_grid' AS source,
        'paris_centre'   AS source_id,
        ts_utc,
        'Open-Meteo grid (Paris centre)' AS source_label,
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
    source,
    source_id,
    ts_utc,
    DATETIME(ts_utc, 'Europe/Paris') AS ts_local_paris,
    DATE(ts_utc, 'Europe/Paris') AS local_date,
    EXTRACT(HOUR FROM DATETIME(ts_utc, 'Europe/Paris')) AS local_hour,
    temp_c,
    source_label,
    latitude,
    longitude,
    elevation_m,
    DATE(ts_utc, 'Europe/Paris') BETWEEN DATE '2026-04-01' AND DATE '2026-04-30' AS is_april_2026
FROM unioned
