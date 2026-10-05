/* @bruin
name: report.atlas_map_layers
type: bq.sql
connection: bruin-playground-arsalan
description: |
  All layers for the dashboard's Vega-Lite event map, per summer season, in one long table (DAC's
  Vega-Lite widget receives a single dataset, so each layer filters on `layer`).
    - coast: basemap vertices from report.atlas_basemap (latitudes > -60).
    - fire: 1 x 1 degree cells with >= 50 MODIS vegetation-fire detections in the window.
    - flood: GDACS flood event centroids overlapping the window.
    - heat: 1M+ cities with >= 28 days of Tmax >= 35 C (heat-exposed).
    - label: the highest risk-score city in each country represented in the top 25,
      with label_side = left when another label city lies within 1,500 km to the east
      (keeps neighbouring labels from colliding).

depends:
  - report.atlas_basemap
  - staging.fire_detections
  - staging.flood_events
  - staging.city_exposure

materialization:
  type: table
  strategy: create+replace

columns:
  - name: layer
    type: VARCHAR
    description: Layer key - coast, fire, flood, heat or label.
    checks:
      - name: accepted_values
        value: [coast, fire, flood, heat, label]
  - name: season_year
    type: INTEGER
    description: Summer season the row belongs to; NULL for the coast layer (shared by all years).
  - name: hazard
    type: VARCHAR
    description: Legend label for point layers (Wildfire hotspot, Flood alert, Extreme heat city); NULL for coast/label.
  - name: segment_id
    type: VARCHAR
    description: Basemap segment id (coast layer only).
  - name: seq
    type: INTEGER
    description: Basemap vertex order (coast layer only).
  - name: lon
    type: DOUBLE
    description: Longitude (WGS84 degrees).
  - name: lat
    type: DOUBLE
    description: Latitude (WGS84 degrees).
  - name: name
    type: VARCHAR
    description: Tooltip / label text (city, country or fire cell).
  - name: detail
    type: VARCHAR
    description: Tooltip detail line with the layer's metric and units.
  - name: label_side
    type: VARCHAR
    description: left or right placement for label-layer text; NULL for other layers.
@bruin */

WITH win AS (
    SELECT season_year, MIN(window_start) AS ws, MAX(window_end) AS we
    FROM staging.city_exposure
    GROUP BY 1
),

top25 AS (
    SELECT *
    FROM staging.city_exposure
    WHERE risk_score IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY season_year ORDER BY risk_score DESC, population_2015 DESC) <= 25
),

label_cities AS (
    SELECT season_year, ghsl_id, city_name, longitude, latitude
    FROM top25
    QUALIFY ROW_NUMBER() OVER (PARTITION BY season_year, country_code ORDER BY risk_score DESC, population_2015 DESC) = 1
)

SELECT 'coast' AS layer, CAST(NULL AS INT64) AS season_year, CAST(NULL AS STRING) AS hazard, segment_id, seq, lon, lat,
    CAST(NULL AS STRING) AS name, CAST(NULL AS STRING) AS detail, CAST(NULL AS STRING) AS label_side
FROM report.atlas_basemap
WHERE lat > -60

UNION ALL

SELECT 'fire', season_year, 'Wildfire hotspot', CAST(NULL AS STRING), CAST(NULL AS INT64),
    lon_cell + 0.5, lat_cell + 0.5,
    CONCAT(
        '1-degree cell centred ',
        CAST(ABS(lat_cell + 0.5) AS STRING), IF(lat_cell + 0.5 >= 0, ' N, ', ' S, '),
        CAST(ABS(lon_cell + 0.5) AS STRING), IF(lon_cell + 0.5 >= 0, ' E', ' W')
    ),
    CONCAT(FORMAT("%'d", detections), ' MODIS vegetation-fire detections'),
    CAST(NULL AS STRING)
FROM (
    SELECT season_year, FLOOR(longitude) AS lon_cell, FLOOR(latitude) AS lat_cell, COUNT(*) AS detections
    FROM staging.fire_detections
    WHERE is_vegetation_fire
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 50
)

UNION ALL

SELECT 'flood', win.season_year, 'Flood alert', CAST(NULL AS STRING), CAST(NULL AS INT64), centroid_lon, centroid_lat,
    CONCAT(event_name, ' (', alert_level, ')'),
    CONCAT(FORMAT_DATE('%b %d', start_date), ' - ', FORMAT_DATE('%b %d', end_date),
        '; reported deaths: ', FORMAT("%'d", deaths), '; displaced: ', FORMAT("%'d", displaced)),
    CAST(NULL AS STRING)
FROM staging.flood_events
JOIN win ON end_date >= win.ws AND start_date <= win.we

UNION ALL

SELECT 'heat', season_year, 'Extreme heat city', CAST(NULL AS STRING), CAST(NULL AS INT64), longitude, latitude,
    city_label,
    CONCAT(CAST(hot_days AS STRING), ' days >= 35 C; peak ', CAST(max_tmax_c AS STRING), ' C'),
    CAST(NULL AS STRING)
FROM staging.city_exposure
WHERE is_heat_exposed

UNION ALL

SELECT 'label', l.season_year, CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS INT64), l.longitude, l.latitude, l.city_name,
    CAST(NULL AS STRING),
    IF(EXISTS (
        SELECT 1 FROM label_cities AS o
        WHERE o.season_year = l.season_year
            AND o.ghsl_id != l.ghsl_id
            AND o.longitude > l.longitude
            AND ST_DISTANCE(ST_GEOGPOINT(o.longitude, o.latitude), ST_GEOGPOINT(l.longitude, l.latitude)) < 1500000
    ), 'left', 'right')
FROM label_cities AS l
