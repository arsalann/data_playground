/* @bruin
name: staging.city_exposure
type: bq.sql
connection: bruin-playground-arsalan
description: |
  One row per 1M+ urban centre per summer season (2019-2026) with its exposure
  to extreme heat, wildfire and flooding, plus a vulnerability-weighted risk score.
  Each season's window is the span of ERA5 days loaded for that year
  (Jun 1 - Sep 20); fire and flood inputs are clipped to the same window.

  Hazard metrics (identical spatial parameters for every city):
    - Heat: days with ERA5 Tmax >= 35 C at the city grid cell (ET-SCI TXge35).
    - Fire: MODIS vegetation-fire detections (confidence >= 30, static sources
      masked) within 50 km of the GHSL centroid; fire_days = distinct dates.
    - Flood: GDACS flood events whose affected-area polygon contains the city
      centroid and whose dates overlap the window.
  Exposure flags: heat and fire need the hazard on at least 1 day in 4 (>= 28 of the
  112 days); flood = any GDACS event covering the city.
  Risk score = 100 * exposure_index * (1 - HDI), where exposure_index is the mean of
  LEAST(hot_days / 56, 1), LEAST(fire_days / 56, 1) and max flood alert rank / 3
  (56 days = half the window). Cities without HDI get a NULL risk score.

depends:
  - raw.atlas_cities
  - staging.city_weather_daily
  - staging.fire_detections
  - staging.flood_events

materialization:
  type: table
  strategy: create+replace

columns:
  - name: ghsl_id
    type: INTEGER
    description: GHSL urban centre identifier.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: season_year
    type: INTEGER
    description: Summer season (calendar year).
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: city_name
    type: VARCHAR
    description: Urban centre name.
  - name: country_code
    type: VARCHAR
    description: ISO3 country code.
  - name: country_name
    type: VARCHAR
    description: Country name.
  - name: city_label
    type: VARCHAR
    description: Display label "City, ISO3".
  - name: latitude
    type: DOUBLE
    description: Centroid latitude (WGS84 degrees).
  - name: longitude
    type: DOUBLE
    description: Centroid longitude (WGS84 degrees).
  - name: population_2015
    type: DOUBLE
    description: GHS-POP modelled population, 2015 (people).
  - name: population_m
    type: DOUBLE
    description: population_2015 in millions of people.
  - name: hdi
    type: DOUBLE
    description: Subnational Human Development Index, 2020 (0-1).
  - name: hdi_imputed
    type: BOOLEAN
    description: TRUE when the GHSL HDI was replaced by the country median (see raw.atlas_cities).
  - name: hdi_tier
    type: VARCHAR
    description: UNDP HDI tier - Low (< 0.550), Medium (0.550-0.699), High (0.700-0.799), Very high (>= 0.800), Unknown.
    checks:
      - name: accepted_values
        value: [Low, Medium, High, Very high, Unknown]
  - name: hdi_tier_order
    type: INTEGER
    description: Sort key for hdi_tier (1 = Low ... 4 = Very high, 5 = Unknown).
  - name: window_start
    type: DATE
    description: First day of the analysis window.
  - name: window_end
    type: DATE
    description: Last day of the analysis window.
  - name: days_observed
    type: INTEGER
    description: ERA5 days available for the city in the window.
  - name: hot_days
    type: INTEGER
    description: Days with Tmax >= 35 C.
  - name: humid_heat_days
    type: INTEGER
    description: Days with apparent Tmax >= 40 C.
  - name: max_tmax_c
    type: DOUBLE
    description: Highest daily maximum temperature in the window (degrees C).
  - name: heavy_rain_days
    type: INTEGER
    description: Days with precipitation >= 50 mm.
  - name: max_daily_precip_mm
    type: DOUBLE
    description: Wettest single day in the window (mm).
  - name: fire_detections_50km
    type: INTEGER
    description: MODIS vegetation-fire detections within 50 km.
  - name: fire_days_50km
    type: INTEGER
    description: Distinct days with at least one vegetation-fire detection within 50 km.
  - name: flood_events
    type: INTEGER
    description: GDACS flood events whose affected area contains the city centroid.
  - name: max_flood_alert
    type: VARCHAR
    description: Highest GDACS alert level among those events (None when no event).
  - name: is_heat_exposed
    type: BOOLEAN
    description: hot_days >= 28 (at least 1 day in 4).
  - name: is_fire_exposed
    type: BOOLEAN
    description: fire_days_50km >= 28 (at least 1 day in 4).
  - name: is_flood_exposed
    type: BOOLEAN
    description: flood_events >= 1.
  - name: hazard_count
    type: INTEGER
    description: Number of hazards the city is exposed to (0-3).
    checks:
      - name: min
        value: 0
      - name: max
        value: 3
  - name: hazard_combo
    type: VARCHAR
    description: Readable list of exposed hazards (e.g. "Heat + Fire"), or "None".
  - name: exposure_index
    type: DOUBLE
    description: Mean of the three capped hazard scores (0-1).
  - name: risk_score
    type: DOUBLE
    description: 100 * exposure_index * (1 - HDI); higher = more hazard exposure in a less-developed city (0-100 scale).
@bruin */

WITH cities AS (
    SELECT *, ST_GEOGPOINT(longitude, latitude) AS geog
    FROM raw.atlas_cities
),

win AS (
    SELECT season_year, MIN(date) AS window_start, MAX(date) AS window_end
    FROM staging.city_weather_daily
    GROUP BY 1
),

weather AS (
    SELECT
        ghsl_id,
        season_year,
        COUNT(*) AS days_observed,
        COUNTIF(is_hot_day) AS hot_days,
        COUNTIF(is_humid_heat_day) AS humid_heat_days,
        MAX(tmax_c) AS max_tmax_c,
        COUNTIF(is_heavy_rain_day) AS heavy_rain_days,
        MAX(precip_mm) AS max_daily_precip_mm
    FROM staging.city_weather_daily
    GROUP BY 1, 2
),

fires AS (
    SELECT
        c.ghsl_id,
        win.season_year,
        COUNT(*) AS fire_detections_50km,
        COUNT(DISTINCT f.acq_date) AS fire_days_50km
    FROM cities AS c
    JOIN staging.fire_detections AS f
        ON f.is_vegetation_fire
        AND ST_DWITHIN(c.geog, f.geog, 50000)
    JOIN win
        ON f.season_year = win.season_year
        AND f.acq_date BETWEEN win.window_start AND win.window_end
    GROUP BY 1, 2
),

floods AS (
    SELECT
        c.ghsl_id,
        win.season_year,
        COUNT(*) AS flood_events,
        MAX(e.alert_rank) AS max_alert_rank
    FROM cities AS c
    CROSS JOIN win
    JOIN staging.flood_events AS e
        ON e.end_date >= win.window_start
        AND e.start_date <= win.window_end
        AND ST_INTERSECTS(c.geog, e.affected_area)
    GROUP BY 1, 2
),

joined AS (
    SELECT
        c.ghsl_id,
        c.city_name,
        c.country_code,
        c.country_name,
        c.latitude,
        c.longitude,
        c.population_2015,
        c.hdi,
        c.hdi_imputed,
        win.season_year,
        win.window_start,
        win.window_end,
        COALESCE(w.days_observed, 0) AS days_observed,
        COALESCE(w.hot_days, 0) AS hot_days,
        COALESCE(w.humid_heat_days, 0) AS humid_heat_days,
        w.max_tmax_c,
        COALESCE(w.heavy_rain_days, 0) AS heavy_rain_days,
        w.max_daily_precip_mm,
        COALESCE(f.fire_detections_50km, 0) AS fire_detections_50km,
        COALESCE(f.fire_days_50km, 0) AS fire_days_50km,
        COALESCE(fl.flood_events, 0) AS flood_events,
        COALESCE(fl.max_alert_rank, 0) AS max_alert_rank
    FROM cities AS c
    CROSS JOIN win
    LEFT JOIN weather AS w USING (ghsl_id, season_year)
    LEFT JOIN fires AS f USING (ghsl_id, season_year)
    LEFT JOIN floods AS fl USING (ghsl_id, season_year)
),

flagged AS (
    SELECT
        *,
        hot_days >= 28 AS is_heat_exposed,
        fire_days_50km >= 28 AS is_fire_exposed,
        flood_events >= 1 AS is_flood_exposed,
        (LEAST(hot_days / 56, 1) + LEAST(fire_days_50km / 56, 1) + max_alert_rank / 3) / 3 AS exposure_index
    FROM joined
)

SELECT
    ghsl_id,
    season_year,
    city_name,
    country_code,
    country_name,
    CONCAT(city_name, ', ', country_code) AS city_label,
    latitude,
    longitude,
    population_2015,
    ROUND(population_2015 / 1e6, 2) AS population_m,
    ROUND(hdi, 3) AS hdi,
    hdi_imputed,
    CASE
        WHEN hdi IS NULL THEN 'Unknown'
        WHEN hdi < 0.55 THEN 'Low'
        WHEN hdi < 0.70 THEN 'Medium'
        WHEN hdi < 0.80 THEN 'High'
        ELSE 'Very high'
    END AS hdi_tier,
    CASE
        WHEN hdi IS NULL THEN 5
        WHEN hdi < 0.55 THEN 1
        WHEN hdi < 0.70 THEN 2
        WHEN hdi < 0.80 THEN 3
        ELSE 4
    END AS hdi_tier_order,
    window_start,
    window_end,
    days_observed,
    hot_days,
    humid_heat_days,
    ROUND(max_tmax_c, 1) AS max_tmax_c,
    heavy_rain_days,
    ROUND(max_daily_precip_mm, 1) AS max_daily_precip_mm,
    fire_detections_50km,
    fire_days_50km,
    flood_events,
    CASE max_alert_rank WHEN 3 THEN 'Red' WHEN 2 THEN 'Orange' WHEN 1 THEN 'Green' ELSE 'None' END AS max_flood_alert,
    is_heat_exposed,
    is_fire_exposed,
    is_flood_exposed,
    CAST(is_heat_exposed AS INT64) + CAST(is_fire_exposed AS INT64) + CAST(is_flood_exposed AS INT64) AS hazard_count,
    COALESCE(
        NULLIF(ARRAY_TO_STRING([
            IF(is_heat_exposed, 'Heat', NULL),
            IF(is_fire_exposed, 'Fire', NULL),
            IF(is_flood_exposed, 'Flood', NULL)
        ], ' + '), ''),
        'None'
    ) AS hazard_combo,
    ROUND(exposure_index, 4) AS exposure_index,
    ROUND(100 * exposure_index * (1 - hdi), 1) AS risk_score
FROM flagged
