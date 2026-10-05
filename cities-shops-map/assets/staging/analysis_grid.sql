/* @bruin
name: staging.analysis_grid
type: bq.sql
connection: bruin-playground-arsalan
description: |
  One 250 m square cell per candidate location, built identically for all five cities so the
  spatial methodology never varies between them.

  Cell identifiers come from an equirectangular metric approximation with a per-city latitude
  reference, computed in SQL:

    cell_x = FLOOR(lon * 111320 * COS(lat_ref_rad) / 250)
    cell_y = FLOOR(lat * 110540 / 250)

  The latitude reference is the centre of each city's bounding box. Using one reference per
  city rather than per row keeps cells square and equal-area within a city; the residual
  distortion across a city 40 km tall is under 0.3% in cell width, which is far below the
  resolution of anything being measured here.

  The grid is restricted to cells containing at least one geocoded, open establishment of any
  shop type. This is deliberate and is the direct analogue of the reference project scoring
  only street-network nodes rather than a blanket raster. A cell with no commercial premises
  at all has no evidence of being usable retail space, and scoring it would put
  recommendations in parks, rail yards and reservoirs.

  Paris's native idcar_200m is carried through separately in staging.shops_unified for
  joining INSEE data, and is never used as the analysis grid.

depends:
  - staging.shops_unified

materialization:
  type: table
  strategy: create+replace

columns:
  - name: city
    type: VARCHAR
    description: City slug.
    primary_key: true
    nullable: false
  - name: cell_x
    type: INTEGER
    description: Grid column index from the equirectangular metric approximation, 250 m per unit.
    primary_key: true
    nullable: false
  - name: cell_y
    type: INTEGER
    description: Grid row index from the equirectangular metric approximation, 250 m per unit.
    primary_key: true
    nullable: false
  - name: cell_id
    type: VARCHAR
    description: Human-readable cell identifier, city and indices concatenated.
  - name: center_lon
    type: DOUBLE
    description: Longitude of the cell centre in decimal degrees, WGS84.
  - name: center_lat
    type: DOUBLE
    description: Latitude of the cell centre in decimal degrees, WGS84.
  - name: lat_ref
    type: DOUBLE
    description: Latitude reference used for this city's grid, the centre of its bounding box.
  - name: establishments_in_cell
    type: INTEGER
    description: Distinct geocoded open establishments of any shop type inside the cell.

@bruin */

WITH grid_params AS (
    SELECT * FROM UNNEST([
        STRUCT('madrid' AS city, 40.4780 AS lat_ref),
        ('paris', 48.8585),
        ('mexico_city', 19.3205),
        ('london', 51.4894),
        ('chicago', 41.8338)
    ])
),

-- One row per establishment, not per (establishment, shop_type), so a Paris bar-cafe is not
-- counted twice when measuring how much commercial fabric a cell holds.
establishments AS (
    SELECT DISTINCT
        s.city,
        s.establishment_id,
        s.lon,
        s.lat
    FROM staging.shops_unified s
    WHERE s.is_open AND s.is_geocoded AND s.is_usable
),

celled AS (
    SELECT
        e.city,
        p.lat_ref,
        CAST(FLOOR(e.lon * 111320 * COS(p.lat_ref * ACOS(-1) / 180) / 250) AS INT64) AS cell_x,
        CAST(FLOOR(e.lat * 110540 / 250) AS INT64) AS cell_y,
        e.establishment_id
    FROM establishments e
    JOIN grid_params p USING (city)
)

SELECT
    city,
    cell_x,
    cell_y,
    CONCAT(city, ':', CAST(cell_x AS STRING), ':', CAST(cell_y AS STRING)) AS cell_id,
    -- Inverse of the forward transform, giving the centre of the cell.
    (cell_x + 0.5) * 250 / (111320 * COS(ANY_VALUE(lat_ref) * ACOS(-1) / 180)) AS center_lon,
    (cell_y + 0.5) * 250 / 110540 AS center_lat,
    ANY_VALUE(lat_ref) AS lat_ref,
    COUNT(DISTINCT establishment_id) AS establishments_in_cell
FROM celled
GROUP BY city, cell_x, cell_y
