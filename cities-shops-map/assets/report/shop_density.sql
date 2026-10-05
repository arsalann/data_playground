/* @bruin
name: report.shop_density
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Observed shop density per 250 m cell: how many establishments of a given type there are per
  1,000 residents in the surrounding 400 m.

  Replaces the former report.site_scores, which blended a demand proxy with a distance-decay
  competition surface into a modelled recommendation of where to open a new shop. That was
  the only part of this project that predicted anything rather than measuring it, and it has
  been removed. What is published here is arithmetic on two counts, both observed:

    shops_per_1k_residents = 1000 * shops_400m / population_400m

  Three deliberate choices, each of which changes what the map says:

  1. **The ratio is withheld below 500 residents within 400 m.** Without a floor, a City of
     London cell with two residents and two cafes reports 937 cafes per 1,000 residents,
     which is arithmetic rather than information. Those cells are published with a null ratio
     and their own map class, because a workplace district with almost no residents is a real
     category, not a data problem to be hidden. The floor removes 1-4.5% of cells per city.
  2. **Zero is its own class, not the bottom of a scale.** A cell with no shops of this type
     within 400 m is qualitatively different from one with few, and quintiling the two
     together would split identical values across bins arbitrarily.
  3. **Bins are value-based, not rank-based.** Cells with the same ratio always land in the
     same class. NTILE would not guarantee that, and a legend that puts two identical values
     in different colours is wrong.

  Density is still computed within (city, shop_type), and the caveats in
  report.coverage_matrix still apply: a low ratio in Mexico City for bars reflects
  under-registration as much as genuine scarcity.

depends:
  - staging.grid_population
  - staging.shops_unified
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
  - name: shop_type
    type: VARCHAR
    description: Canonical shop type.
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
    description: Estimated residents within 400 m of the cell centre.
  - name: shops_400m
    type: INTEGER
    description: Distinct open establishments of this shop type within 400 m of the cell centre.
  - name: all_shops_400m
    type: INTEGER
    description: Distinct open establishments of any shop type within 400 m, for context.
  - name: shops_per_1k_residents
    type: DOUBLE
    description: 1000 * shops_400m / population_400m. Null where population_400m is below 500, because the ratio is not meaningful on a denominator that small.
  - name: has_enough_residents
    type: BOOLEAN
    description: False where population_400m is below 500, meaning the ratio is withheld and the cell is mapped as a low-residential area instead.
  - name: density_class
    type: INTEGER
    description: Map class. 0 means no shops of this type within 400 m, 1-4 are quartiles of the published ratio among cells that have at least one, and null means too few residents to rate.
  - name: density_rank
    type: INTEGER
    description: Rank of the cell within its city and shop type by shops_per_1k_residents, 1 is the highest density. Null where the ratio is withheld.

@bruin */

WITH mappable AS (
    SELECT city, shop_type
    FROM report.coverage_matrix
    WHERE is_mappable
),

grid AS (
    SELECT
        g.*,
        ST_GEOGPOINT(g.center_lon, g.center_lat) AS center_point
    FROM staging.grid_population g
),

-- One point per (establishment, shop_type). An establishment that a register maps to two
-- shop types counts towards the density of both, which is correct.
typed_points AS (
    SELECT
        s.city,
        s.shop_type,
        s.establishment_id,
        ST_GEOGPOINT(ANY_VALUE(s.lon), ANY_VALUE(s.lat)) AS point
    FROM staging.shops_unified s
    JOIN mappable USING (city, shop_type)
    WHERE s.is_open AND s.is_geocoded AND s.is_usable
    GROUP BY s.city, s.shop_type, s.establishment_id
),

cell_type AS (
    SELECT
        g.city,
        m.shop_type,
        g.cell_x,
        g.cell_y,
        g.cell_id,
        g.center_lon,
        g.center_lat,
        g.center_point,
        g.population_400m,
        g.establishments_400m,
        g.has_enough_residents
    FROM grid g
    JOIN mappable m ON m.city = g.city
),

counted AS (
    SELECT
        c.city,
        c.shop_type,
        c.cell_x,
        c.cell_y,
        COUNT(DISTINCT p.establishment_id) AS shops_400m
    FROM cell_type c
    JOIN typed_points p
        ON p.city = c.city
        AND p.shop_type = c.shop_type
        AND ST_DWITHIN(c.center_point, p.point, 400)
    GROUP BY c.city, c.shop_type, c.cell_x, c.cell_y
),

assembled AS (
    SELECT
        c.city,
        c.shop_type,
        c.cell_x,
        c.cell_y,
        c.cell_id,
        c.center_lon,
        c.center_lat,
        c.population_400m,
        COALESCE(k.shops_400m, 0) AS shops_400m,
        c.establishments_400m AS all_shops_400m,
        c.has_enough_residents,
        IF(
            c.has_enough_residents,
            SAFE_DIVIDE(1000.0 * COALESCE(k.shops_400m, 0), c.population_400m),
            NULL
        ) AS shops_per_1k_residents
    FROM cell_type c
    LEFT JOIN counted k
        ON k.city = c.city
        AND k.shop_type = c.shop_type
        AND k.cell_x = c.cell_x
        AND k.cell_y = c.cell_y
),

-- Value-based quartile thresholds over the cells that actually have a shop of this type, so
-- two cells with the same ratio always receive the same class.
thresholds AS (
    SELECT
        city,
        shop_type,
        APPROX_QUANTILES(shops_per_1k_residents, 4)[OFFSET(1)] AS q1,
        APPROX_QUANTILES(shops_per_1k_residents, 4)[OFFSET(2)] AS q2,
        APPROX_QUANTILES(shops_per_1k_residents, 4)[OFFSET(3)] AS q3
    FROM assembled
    WHERE shops_per_1k_residents > 0
    GROUP BY city, shop_type
)

SELECT
    a.city,
    a.shop_type,
    a.cell_x,
    a.cell_y,
    a.cell_id,
    a.center_lon,
    a.center_lat,
    a.population_400m,
    a.shops_400m,
    a.all_shops_400m,
    a.shops_per_1k_residents,
    a.has_enough_residents,
    CASE
        WHEN NOT a.has_enough_residents THEN NULL
        WHEN a.shops_400m = 0 THEN 0
        WHEN a.shops_per_1k_residents <= t.q1 THEN 1
        WHEN a.shops_per_1k_residents <= t.q2 THEN 2
        WHEN a.shops_per_1k_residents <= t.q3 THEN 3
        ELSE 4
    END AS density_class,
    IF(
        a.has_enough_residents,
        CAST(RANK() OVER (
            PARTITION BY a.city, a.shop_type, a.has_enough_residents
            ORDER BY a.shops_per_1k_residents DESC
        ) AS INT64),
        NULL
    ) AS density_rank
FROM assembled a
LEFT JOIN thresholds t ON t.city = a.city AND t.shop_type = a.shop_type
