/* @bruin
name: staging.grid_population
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Resident population and total commercial density within 400 m of each 250 m cell centre,
  identical in definition for all five cities.

  Replaces the former staging.demand_grid. That asset existed to feed a modelled site score
  and carried a third input, rail and metro proximity, weighted into a demand z-score. The
  score has been removed in favour of an observed measure, so the station input has been
  removed with it rather than left in place feeding nothing.

  Why a 400 m catchment rather than the cell itself: a commercial street cell can hold a
  dozen shops and almost no residents, and reading its population as zero would be wrong.
  400 m is roughly a five-minute walk and is the radius at which retail catchment is
  normally measured. The same radius is used for population and for shop counts, so the
  numerator and denominator of the published ratio describe the same neighbourhood.

depends:
  - staging.analysis_grid
  - staging.shops_unified
  - raw.ghsl_population_100m

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
    description: Grid column index.
    primary_key: true
    nullable: false
  - name: cell_y
    type: INTEGER
    description: Grid row index.
    primary_key: true
    nullable: false
  - name: cell_id
    type: VARCHAR
    description: Human-readable cell identifier.
  - name: center_lon
    type: DOUBLE
    description: Longitude of the cell centre in decimal degrees, WGS84.
  - name: center_lat
    type: DOUBLE
    description: Latitude of the cell centre in decimal degrees, WGS84.
  - name: population_400m
    type: DOUBLE
    description: Estimated residents within 400 m of the cell centre, from GHS-POP 2025 100 m cells.
  - name: establishments_400m
    type: INTEGER
    description: Distinct open establishments of any shop type within 400 m of the cell centre.
  - name: establishments_in_cell
    type: INTEGER
    description: Distinct open establishments of any shop type inside the cell itself.
  - name: has_enough_residents
    type: BOOLEAN
    description: True where population_400m is at least 500, the floor below which a per-resident ratio is not published. Below that the denominator is too small for the ratio to mean anything.

@bruin */

WITH grid AS (
    SELECT
        city,
        cell_x,
        cell_y,
        cell_id,
        center_lon,
        center_lat,
        establishments_in_cell,
        ST_GEOGPOINT(center_lon, center_lat) AS center_point
    FROM staging.analysis_grid
),

population_points AS (
    SELECT
        city,
        population,
        ST_GEOGPOINT(lon, lat) AS point
    FROM raw.ghsl_population_100m
    WHERE population > 0
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY city, moll_x, moll_y ORDER BY extracted_at DESC
    ) = 1
),

-- Deduplicated before the point is constructed: BigQuery rejects GEOGRAPHY in SELECT DISTINCT.
establishment_points AS (
    SELECT
        city,
        establishment_id,
        ST_GEOGPOINT(ANY_VALUE(lon), ANY_VALUE(lat)) AS point
    FROM staging.shops_unified
    WHERE is_open AND is_geocoded AND is_usable
    GROUP BY city, establishment_id
),

population_catchment AS (
    SELECT
        g.city,
        g.cell_x,
        g.cell_y,
        SUM(p.population) AS population_400m
    FROM grid g
    JOIN population_points p
        ON p.city = g.city AND ST_DWITHIN(g.center_point, p.point, 400)
    GROUP BY g.city, g.cell_x, g.cell_y
),

commercial_catchment AS (
    SELECT
        g.city,
        g.cell_x,
        g.cell_y,
        COUNT(DISTINCT e.establishment_id) AS establishments_400m
    FROM grid g
    JOIN establishment_points e
        ON e.city = g.city AND ST_DWITHIN(g.center_point, e.point, 400)
    GROUP BY g.city, g.cell_x, g.cell_y
)

SELECT
    g.city,
    g.cell_x,
    g.cell_y,
    g.cell_id,
    g.center_lon,
    g.center_lat,
    COALESCE(p.population_400m, 0.0) AS population_400m,
    COALESCE(c.establishments_400m, 0) AS establishments_400m,
    g.establishments_in_cell,
    COALESCE(p.population_400m, 0.0) >= 500 AS has_enough_residents
FROM grid g
LEFT JOIN population_catchment p USING (city, cell_x, cell_y)
LEFT JOIN commercial_catchment c USING (city, cell_x, cell_y)
