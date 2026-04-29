/* @bruin

name: polymarket_weather_staging.temperature_daily
type: bq.sql
description: |
  Daily temperature panel for the six Paris-region stations plus the Open-Meteo grid,
  used for both Polymarket-style daily-max resolution counterfactuals and for
  multi-year climatological context in the April 2026 temperature sensor tampering investigation.

  This dataset is critical for forensic weather analysis, providing the ground truth for
  counterfactual market resolution calculations. Each row represents a single station-day
  with temperature extremes that would have determined Polymarket betting outcomes.

  Two source paths are unioned with preference logic:
    1. Pre-aggregated daily series from raw.station_daily (2010+ climatology baseline)
    2. Daily max/min/mean derived from hourly observations (raw.station_hourly +
       raw.openmeteo_grid) — this is the *Polymarket-equivalent* daily max, computed
       from local-day hourly readings using Europe/Paris timezone

  When both sources exist for the same (station, date), the hourly-derived row is
  preferred because it precisely matches what Polymarket's resolution logic would have
  observed from real-time feeds. The dataset spans 2010-2026 with ~36k station-days
  covering normal climatology plus the controversial April 2026 period.

  Station coverage includes all major Paris-area microclimates: airports (CDG-07157 suspect
  sensor, Orly-07149, Le Bourget-07150), urban core (Montsouris-07156), military
  (Villacoublay-07145), and semi-rural (Trappes-07145). Open-Meteo gridded reanalysis
  at Paris centre provides an independent baseline unaffected by any single sensor anomaly.
connection: bruin-playground-arsalan
tags:
  - domain:forensic_investigation
  - domain:finance
  - data_type:fact_table
  - data_type:time_series
  - source:meteostat
  - source:openmeteo
  - geography:paris_region
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
  - name: source
    type: VARCHAR
    description: Data source type - either 'meteostat' for physical weather station observations or 'openmeteo_grid' for ERA5-based gridded reanalysis. Cardinality is exactly 2 values.
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
    description: Station identifier - 5-digit Meteostat/WMO station codes (07157=CDG suspect sensor, 07149=Orly, 07150=Le Bourget, 07156=Montsouris, 07145=Villacoublay/Trappes) or 'paris_centre' for the Open-Meteo grid point. Cardinality is exactly 7 values.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: local_date
    type: DATE
    description: Calendar date in Europe/Paris local time (CET/CEST). Spans 2010-01-01 through 2026-04-29 covering both climatology baseline and the controversial April 2026 period. Primary temporal dimension for counterfactual analysis.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: source_label
    type: VARCHAR
    description: Human-readable station name including location context (e.g. "Paris–Charles de Gaulle", "Open-Meteo grid (Paris centre)"). Used for dashboard displays and includes airport codes where applicable.
    nullable: false
    checks:
      - name: not_null
  - name: temp_max_c
    type: DOUBLE
    description: Daily maximum temperature in degrees Celsius, rounded to 2 decimal places. This is the critical value for Polymarket daily-max market resolution. Range approximately -4.2°C to 41.9°C. Null values (~2.8%) indicate missing sensor data or quality control exclusions.
  - name: temp_min_c
    type: DOUBLE
    description: Daily minimum temperature in degrees Celsius, rounded to 2 decimal places. Range approximately -13.3°C to 26°C. Used for diurnal range analysis and temperature anomaly detection. Null values (~2.8%) correlate with temp_max_c missingness.
  - name: temp_mean_c
    type: DOUBLE
    description: Daily mean temperature in degrees Celsius, rounded to 2 decimal places. Computed as average of hourly readings (hourly_derived) or from pre-aggregated daily mean (station_daily). Range approximately -7.6°C to 33.9°C. More complete than max/min (~1.2% null rate) due to different calculation methods.
  - name: temp_max_bucket_c
    type: INTEGER
    description: Daily maximum temperature rounded to whole degrees Celsius - the exact integer bucket Polymarket would use to resolve daily-max temperature markets. This field directly maps to betting outcomes and determines winning/losing positions. Null when temp_max_c is null.
  - name: derivation
    type: VARCHAR
    description: Data lineage flag indicating computation method - 'hourly_derived' (aggregated from raw hourly readings, preferred for Polymarket accuracy) or 'station_daily' (pre-aggregated daily values, used for climatology gaps). Cardinality is exactly 2 values.
    nullable: false
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - hourly_derived
          - station_daily

@bruin */

WITH station_daily_dedup AS (
    SELECT * EXCEPT(rn) FROM (
        SELECT
            station_id, date, station_name,
            temp_max_c, temp_min_c, temp_mean_c,
            ROW_NUMBER() OVER (PARTITION BY station_id, date ORDER BY extracted_at DESC) AS rn
        FROM `bruin-playground-arsalan.polymarket_weather_raw.station_daily`
    )
    WHERE rn = 1
),

hourly_station AS (
    SELECT
        'meteostat' AS source,
        station_id  AS source_id,
        DATE(ts_utc, 'Europe/Paris') AS local_date,
        ANY_VALUE(station_name) AS source_label,
        MAX(temp_c) AS temp_max_c,
        MIN(temp_c) AS temp_min_c,
        AVG(temp_c) AS temp_mean_c
    FROM `bruin-playground-arsalan.polymarket_weather_raw.station_hourly`
    WHERE temp_c IS NOT NULL
    GROUP BY 1, 2, 3
),

hourly_grid AS (
    SELECT
        'openmeteo_grid' AS source,
        'paris_centre'   AS source_id,
        DATE(ts_utc, 'Europe/Paris') AS local_date,
        'Open-Meteo grid (Paris centre)' AS source_label,
        MAX(temp_c) AS temp_max_c,
        MIN(temp_c) AS temp_min_c,
        AVG(temp_c) AS temp_mean_c
    FROM `bruin-playground-arsalan.polymarket_weather_raw.openmeteo_grid`
    WHERE temp_c IS NOT NULL
    GROUP BY 1, 2, 3
),

prefer_hourly AS (
    SELECT * FROM hourly_station
    UNION ALL
    SELECT * FROM hourly_grid
),

station_daily_only AS (
    -- Rows from station_daily where we don't already have an hourly-derived row
    SELECT
        'meteostat' AS source,
        sd.station_id AS source_id,
        sd.date AS local_date,
        sd.station_name AS source_label,
        sd.temp_max_c,
        sd.temp_min_c,
        sd.temp_mean_c
    FROM station_daily_dedup sd
    LEFT JOIN hourly_station h
        ON sd.station_id = h.source_id AND sd.date = h.local_date
    WHERE h.local_date IS NULL
),

unioned AS (
    SELECT *, 'hourly_derived' AS derivation FROM prefer_hourly
    UNION ALL
    SELECT *, 'station_daily' AS derivation FROM station_daily_only
)

SELECT
    source,
    source_id,
    local_date,
    source_label,
    ROUND(temp_max_c, 2) AS temp_max_c,
    ROUND(temp_min_c, 2) AS temp_min_c,
    ROUND(temp_mean_c, 2) AS temp_mean_c,
    CAST(ROUND(temp_max_c) AS INT64) AS temp_max_bucket_c,
    derivation
FROM unioned
