/* @bruin
name: report.shop_points
type: bq.sql
connection: bruin-playground-arsalan
description: |
  The establishment point layer for the map: one row per mappable open establishment and
  shop type, carrying the grid cell it sits in and the separability grade of its
  classification.

  Only open, geocoded establishments of scoreable city and shop-type combinations appear
  here. Establishments the source could not geocode are excluded from the map but are counted
  in report.coverage_matrix, so the reader can see how many premises are missing from what
  they are looking at rather than being shown a map that silently omits 10% of London.

depends:
  - staging.shops_unified
  - staging.analysis_grid
  - report.coverage_matrix

materialization:
  type: table
  strategy: create+replace

columns:
  - name: city
    type: VARCHAR
    description: City slug.
    primary_key: true
    nullable: false
  - name: establishment_id
    type: VARCHAR
    description: Source natural key for the premise.
    primary_key: true
    nullable: false
  - name: shop_type
    type: VARCHAR
    description: Canonical shop type.
    primary_key: true
    nullable: false
  - name: name
    type: VARCHAR
    description: Trading name as published, frequently blank in Madrid and Paris.
  - name: native_label
    type: VARCHAR
    description: Source activity label, in the source language, so the classification is auditable from the tooltip.
  - name: separability
    type: VARCHAR
    description: How cleanly the source code isolates this shop type, used to shade uncertain points on the map.
  - name: lon
    type: DOUBLE
    description: Longitude in decimal degrees, WGS84.
  - name: lat
    type: DOUBLE
    description: Latitude in decimal degrees, WGS84.
  - name: cell_x
    type: INTEGER
    description: Grid column index of the containing 250 m cell.
  - name: cell_y
    type: INTEGER
    description: Grid row index of the containing 250 m cell.
  - name: floor_area_m2
    type: DOUBLE
    description: Interior floor area in square metres. Paris only, and present on under 2% of premises.

@bruin */

WITH grid_params AS (
    SELECT city, ANY_VALUE(lat_ref) AS lat_ref
    FROM staging.analysis_grid
    GROUP BY city
),

scoreable AS (
    SELECT city, shop_type
    FROM report.coverage_matrix
    WHERE is_mappable
)

SELECT
    s.city,
    s.establishment_id,
    s.shop_type,
    s.name,
    s.native_label,
    s.separability,
    s.lon,
    s.lat,
    CAST(FLOOR(s.lon * 111320 * COS(p.lat_ref * ACOS(-1) / 180) / 250) AS INT64) AS cell_x,
    CAST(FLOOR(s.lat * 110540 / 250) AS INT64) AS cell_y,
    s.floor_area_m2
FROM staging.shops_unified s
JOIN scoreable USING (city, shop_type)
JOIN grid_params p ON p.city = s.city
WHERE s.is_open AND s.is_geocoded AND s.is_usable
