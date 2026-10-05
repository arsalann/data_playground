/* @bruin
name: report.atlas_basemap
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Simplified world coastline vertices used as the basemap for the dashboard's
  Vega-Lite event map (DAC's Vega-Lite widget only accepts query data, so the
  outline is drawn as ordered line vertices rather than loaded from a URL).
  Built from Overture Maps country land areas (bigquery-public-data.overture_maps,
  ODbL / CDLA-Permissive) - unioned, simplified to 30 km tolerance, polygons
  under 5,000 km2 dropped, and exterior rings split wherever they cross the
  antimeridian so no line wraps across the map.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: segment_id
    type: VARCHAR
    description: Line segment identifier (polygon index + antimeridian split index).
    primary_key: true
  - name: seq
    type: INTEGER
    description: Vertex order within the segment.
    primary_key: true
  - name: lon
    type: DOUBLE
    description: Vertex longitude (WGS84 degrees).
  - name: lat
    type: DOUBLE
    description: Vertex latitude (WGS84 degrees).
@bruin */

WITH land AS (
    SELECT ST_SIMPLIFY(ST_UNION_AGG(geometry), 30000) AS g
    FROM `bigquery-public-data.overture_maps.division_area`
    WHERE subtype = 'country' AND class = 'land'
),

polygons AS (
    SELECT poly_idx, poly
    FROM land, UNNEST(ST_DUMP(g, 2)) AS poly WITH OFFSET AS poly_idx
    WHERE ST_AREA(poly) > 5e9
),

rings AS (
    SELECT poly_idx, ST_EXTERIORRING(poly) AS ring
    FROM polygons
),

vertices AS (
    SELECT
        poly_idx,
        pt_idx,
        ST_X(ST_POINTN(ring, pt_idx)) AS lon,
        ST_Y(ST_POINTN(ring, pt_idx)) AS lat
    FROM rings, UNNEST(GENERATE_ARRAY(1, ST_NUMPOINTS(ring))) AS pt_idx
),

flagged AS (
    SELECT
        *,
        IF(ABS(lon - LAG(lon) OVER (PARTITION BY poly_idx ORDER BY pt_idx)) > 180, 1, 0) AS is_break
    FROM vertices
),

segmented AS (
    SELECT
        *,
        SUM(is_break) OVER (PARTITION BY poly_idx ORDER BY pt_idx) AS split_idx
    FROM flagged
)

SELECT
    CONCAT(CAST(poly_idx AS STRING), '_', CAST(split_idx AS STRING)) AS segment_id,
    CAST(pt_idx AS INT64) AS seq,
    ROUND(lon, 3) AS lon,
    ROUND(lat, 3) AS lat
FROM segmented
