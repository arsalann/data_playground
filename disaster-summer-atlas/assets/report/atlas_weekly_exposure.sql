/* @bruin
name: report.atlas_weekly_exposure
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Seasonal timeline: for each ISO week of each summer season (2019-2026), the population
  (millions, GHS-POP 2015) of 1M+ cities exposed to each hazard that week.
  Weekly rules scale the season rule (hazard on >= 1 day in 4) to one week:
    - Heat: >= 2 of 7 days with Tmax >= 35 C.
    - Fire: >= 2 of 7 days with a vegetation-fire detection within 50 km.
    - Flood: a GDACS flood event covering the city is active on any day of the week.

depends:
  - raw.atlas_cities
  - staging.city_weather_daily
  - staging.fire_detections
  - staging.flood_events

materialization:
  type: table
  strategy: create+replace

columns:
  - name: week_start
    type: DATE
    description: Monday of the ISO week.
    primary_key: true
  - name: season_year
    type: INTEGER
    description: Summer season (calendar year).
  - name: week_of_season
    type: INTEGER
    description: Week number within the season (1 = week starting on or before Jun 1).
  - name: week_label
    type: VARCHAR
    description: Display label for the week (e.g. "Jun 01").
  - name: heat_pop_m
    type: DOUBLE
    description: Population (millions) in cities with >= 2 hot days that week.
  - name: fire_pop_m
    type: DOUBLE
    description: Population (millions) in cities with >= 2 fire-days within 50 km that week.
  - name: flood_pop_m
    type: DOUBLE
    description: Population (millions) in cities inside an active GDACS flood area that week.
  - name: heat_cities
    type: INTEGER
    description: Number of cities meeting the weekly heat rule.
  - name: fire_cities
    type: INTEGER
    description: Number of cities meeting the weekly fire rule.
  - name: flood_cities
    type: INTEGER
    description: Number of cities meeting the weekly flood rule.
@bruin */

WITH cities AS (
    SELECT ghsl_id, population_2015, ST_GEOGPOINT(longitude, latitude) AS geog
    FROM raw.atlas_cities
),

weeks AS (
    SELECT DISTINCT season_year, week_start
    FROM staging.city_weather_daily
),

heat AS (
    SELECT ghsl_id, week_start
    FROM staging.city_weather_daily
    GROUP BY 1, 2
    HAVING COUNTIF(is_hot_day) >= 2
),

fire AS (
    SELECT c.ghsl_id, f.week_start
    FROM cities AS c
    JOIN staging.fire_detections AS f
        ON f.is_vegetation_fire
        AND ST_DWITHIN(c.geog, f.geog, 50000)
    JOIN weeks AS w
        ON w.week_start = f.week_start
    GROUP BY 1, 2
    HAVING COUNT(DISTINCT f.acq_date) >= 2
),

flood AS (
    SELECT DISTINCT c.ghsl_id, w.week_start
    FROM cities AS c
    CROSS JOIN weeks AS w
    JOIN staging.flood_events AS e
        ON e.start_date <= DATE_ADD(w.week_start, INTERVAL 6 DAY)
        AND e.end_date >= w.week_start
        AND ST_INTERSECTS(c.geog, e.affected_area)
),

grid AS (
    SELECT
        w.season_year,
        w.week_start,
        c.ghsl_id,
        c.population_2015,
        h.ghsl_id IS NOT NULL AS heat,
        fi.ghsl_id IS NOT NULL AS fire,
        fl.ghsl_id IS NOT NULL AS flood
    FROM weeks AS w
    CROSS JOIN cities AS c
    LEFT JOIN heat AS h ON h.ghsl_id = c.ghsl_id AND h.week_start = w.week_start
    LEFT JOIN fire AS fi ON fi.ghsl_id = c.ghsl_id AND fi.week_start = w.week_start
    LEFT JOIN flood AS fl ON fl.ghsl_id = c.ghsl_id AND fl.week_start = w.week_start
)

SELECT
    season_year,
    DENSE_RANK() OVER (PARTITION BY season_year ORDER BY week_start) AS week_of_season,
    week_start,
    FORMAT_DATE('%b %d', week_start) AS week_label,
    ROUND(SUM(IF(heat, population_2015, 0)) / 1e6, 1) AS heat_pop_m,
    ROUND(SUM(IF(fire, population_2015, 0)) / 1e6, 1) AS fire_pop_m,
    ROUND(SUM(IF(flood, population_2015, 0)) / 1e6, 1) AS flood_pop_m,
    COUNTIF(heat) AS heat_cities,
    COUNTIF(fire) AS fire_cities,
    COUNTIF(flood) AS flood_cities
FROM grid
GROUP BY season_year, week_start
ORDER BY week_start
