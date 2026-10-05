/* @bruin
name: staging.city_weather_daily
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Deduplicated daily ERA5 weather per city with heat and heavy-rain flags.
  Thresholds follow ET-SCI user-defined climate-extremes indices:
  TXge35 (daily maximum temperature >= 35 C) and R50mm (daily precipitation
  >= 50 mm). Apparent temperature >= 40 C is kept as a humid-heat flag.

depends:
  - raw.era5_city_daily

materialization:
  type: table
  strategy: create+replace

columns:
  - name: ghsl_id
    type: INTEGER
    description: GHSL urban centre identifier.
    primary_key: true
    nullable: false
  - name: date
    type: DATE
    description: Local calendar date.
    primary_key: true
    nullable: false
  - name: season_year
    type: INTEGER
    description: Calendar year of date (one summer season per year).
  - name: week_start
    type: DATE
    description: Monday of the ISO week containing date.
  - name: tmax_c
    type: DOUBLE
    description: Daily maximum 2 m temperature (degrees C).
  - name: apparent_tmax_c
    type: DOUBLE
    description: Daily maximum apparent temperature (degrees C).
  - name: precip_mm
    type: DOUBLE
    description: Daily precipitation (mm), 0 when missing.
  - name: is_hot_day
    type: BOOLEAN
    description: TRUE when tmax_c >= 35 (ET-SCI TXge35).
  - name: is_humid_heat_day
    type: BOOLEAN
    description: TRUE when apparent_tmax_c >= 40.
  - name: is_heavy_rain_day
    type: BOOLEAN
    description: TRUE when precip_mm >= 50 (ET-SCI R50mm).
@bruin */

WITH deduped AS (
    SELECT *
    FROM raw.era5_city_daily
    WHERE ghsl_id IS NOT NULL AND date IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ghsl_id, date ORDER BY extracted_at DESC) = 1
)

SELECT
    ghsl_id,
    date,
    EXTRACT(YEAR FROM date) AS season_year,
    DATE_TRUNC(date, ISOWEEK) AS week_start,
    tmax_c,
    apparent_tmax_c,
    COALESCE(precip_mm, 0) AS precip_mm,
    COALESCE(tmax_c >= 35, FALSE) AS is_hot_day,
    COALESCE(apparent_tmax_c >= 40, FALSE) AS is_humid_heat_day,
    COALESCE(precip_mm >= 50, FALSE) AS is_heavy_rain_day
FROM deduped
