/* @bruin
name: staging.weather_daily
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Transforms raw Berlin weather data into an analysis-ready daily table.
  Classifies each day by weather type using WMO weather codes and actual
  precipitation/snowfall measurements. Adds temporal dimensions (year, month,
  season, day of week) and derived metrics (sunshine hours, boolean weather flags).

  WMO weather codes: https://www.nodc.noaa.gov/archive/arc0021/0002199/1.1/data/0-data/HTML/WMO-CODE/WMO4677.HTM

depends:
  - raw.weather_raw

materialization:
  type: table
  strategy: create+replace

columns:
  - name: date
    type: DATE
    description: Observation date
    primary_key: true
    nullable: false
  - name: year
    type: INTEGER
    description: Year extracted from date
  - name: month
    type: INTEGER
    description: Month extracted from date (1-12)
  - name: day_of_week
    type: INTEGER
    description: Day of week (0=Sunday, 6=Saturday)
  - name: season
    type: VARCHAR
    description: Meteorological season (Winter, Spring, Summer, Autumn)
  - name: weather_code
    type: INTEGER
    description: Original WMO weather interpretation code
  - name: weather_description
    type: VARCHAR
    description: Human-readable description of the WMO weather code
  - name: weather_category
    type: VARCHAR
    description: Simplified mutually exclusive category (Clear, Overcast, Rainy, Snowy, Mixed). Overcast = 0 sunshine hours that day. Clear = any sunshine at all.
  - name: temp_max_c
    type: DOUBLE
    description: Maximum temperature in degrees Celsius
  - name: temp_min_c
    type: DOUBLE
    description: Minimum temperature in degrees Celsius
  - name: temp_mean_c
    type: DOUBLE
    description: Mean temperature in degrees Celsius
  - name: precipitation_mm
    type: DOUBLE
    description: Total precipitation in millimeters
  - name: rain_mm
    type: DOUBLE
    description: Rain amount in millimeters
  - name: snowfall_cm
    type: DOUBLE
    description: Snowfall amount in centimeters
  - name: precipitation_hours
    type: DOUBLE
    description: Hours with precipitation
  - name: wind_max_kmh
    type: DOUBLE
    description: Maximum wind speed in km/h
  - name: sunshine_hours
    type: DOUBLE
    description: Sunshine duration in hours (converted from seconds)
  - name: has_rain
    type: BOOLEAN
    description: Whether any measurable rain fell (rain_sum > 0)
  - name: has_snow
    type: BOOLEAN
    description: Whether any measurable snow fell (snowfall_sum > 0)
  - name: has_precipitation
    type: BOOLEAN
    description: Whether any precipitation occurred
  - name: is_overcast
    type: BOOLEAN
    description: Whether the day had zero sunshine hours

@bruin */

SELECT
    CAST(time AS DATE) AS date,
    EXTRACT(YEAR FROM CAST(time AS DATE)) AS year,
    EXTRACT(MONTH FROM CAST(time AS DATE)) AS month,
    EXTRACT(DAYOFWEEK FROM CAST(time AS DATE)) AS day_of_week,

    CASE
        WHEN EXTRACT(MONTH FROM CAST(time AS DATE)) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM CAST(time AS DATE)) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM CAST(time AS DATE)) IN (6, 7, 8) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM CAST(time AS DATE)) IN (9, 10, 11) THEN 'Autumn'
    END AS season,

    weather_code,

    CASE weather_code
        WHEN 0 THEN 'Clear sky'
        WHEN 1 THEN 'Mainly clear'
        WHEN 2 THEN 'Partly cloudy'
        WHEN 3 THEN 'Overcast'
        WHEN 45 THEN 'Fog'
        WHEN 48 THEN 'Depositing rime fog'
        WHEN 51 THEN 'Light drizzle'
        WHEN 53 THEN 'Moderate drizzle'
        WHEN 55 THEN 'Dense drizzle'
        WHEN 56 THEN 'Light freezing drizzle'
        WHEN 57 THEN 'Dense freezing drizzle'
        WHEN 61 THEN 'Slight rain'
        WHEN 63 THEN 'Moderate rain'
        WHEN 65 THEN 'Heavy rain'
        WHEN 66 THEN 'Light freezing rain'
        WHEN 67 THEN 'Heavy freezing rain'
        WHEN 71 THEN 'Slight snowfall'
        WHEN 73 THEN 'Moderate snowfall'
        WHEN 75 THEN 'Heavy snowfall'
        WHEN 77 THEN 'Snow grains'
        WHEN 80 THEN 'Slight rain showers'
        WHEN 81 THEN 'Moderate rain showers'
        WHEN 82 THEN 'Violent rain showers'
        WHEN 85 THEN 'Slight snow showers'
        WHEN 86 THEN 'Heavy snow showers'
        WHEN 95 THEN 'Thunderstorm'
        WHEN 96 THEN 'Thunderstorm with slight hail'
        WHEN 99 THEN 'Thunderstorm with heavy hail'
        ELSE 'Unknown'
    END AS weather_description,

    CASE
        WHEN COALESCE(snowfall_sum, 0) > 0 AND COALESCE(rain_sum, 0) > 0 THEN 'Mixed'
        WHEN COALESCE(snowfall_sum, 0) > 0 THEN 'Snowy'
        WHEN COALESCE(rain_sum, 0) > 0 THEN 'Rainy'
        WHEN COALESCE(sunshine_duration, 0) = 0 THEN 'Overcast'
        ELSE 'Clear'
    END AS weather_category,

    temperature_2m_max AS temp_max_c,
    temperature_2m_min AS temp_min_c,
    temperature_2m_mean AS temp_mean_c,
    COALESCE(precipitation_sum, 0) AS precipitation_mm,
    COALESCE(rain_sum, 0) AS rain_mm,
    COALESCE(snowfall_sum, 0) AS snowfall_cm,
    COALESCE(precipitation_hours, 0) AS precipitation_hours,
    wind_speed_10m_max AS wind_max_kmh,
    ROUND(COALESCE(sunshine_duration, 0) / 3600.0, 2) AS sunshine_hours,

    COALESCE(rain_sum, 0) > 0 AS has_rain,
    COALESCE(snowfall_sum, 0) > 0 AS has_snow,
    COALESCE(precipitation_sum, 0) > 0 AS has_precipitation,
    COALESCE(sunshine_duration, 0) = 0 AS is_overcast

FROM raw.weather_raw
WHERE time IS NOT NULL
ORDER BY date
