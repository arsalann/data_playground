/* @bruin
name: report.density_bins
type: bq.sql
connection: bruin-playground-arsalan
description: |
  The actual value range of each map class, per city and shop type, so the legend states real
  numbers instead of "quartile 3".

  A choropleth legend that names its bins by rank rather than by value tells the reader
  nothing about magnitude, and it hides that the same colour means 0.2 bakeries per 1,000
  residents in one city and 2.0 in another. This table is small (five classes times the
  mappable combinations) and the map reads it directly.

depends:
  - report.shop_density

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
  - name: density_class
    type: INTEGER
    description: Map class. 0 is none within 400 m, 1-4 are quartiles of the published ratio, -1 is the low-residential class where the ratio is withheld.
    primary_key: true
    nullable: false
  - name: cells
    type: INTEGER
    description: Number of cells in this class.
  - name: min_value
    type: DOUBLE
    description: Lowest shops per 1,000 residents in this class. Null for the low-residential class.
  - name: max_value
    type: DOUBLE
    description: Highest shops per 1,000 residents in this class. Null for the low-residential class.
  - name: median_shops_400m
    type: DOUBLE
    description: Median count of this shop type within 400 m for cells in this class, so the reader can sanity-check the ratio against a raw count.
  - name: median_population_400m
    type: DOUBLE
    description: Median residents within 400 m for cells in this class.

@bruin */

SELECT
    city,
    shop_type,
    -- The withheld-ratio cells are given class -1 here so the legend can describe them
    -- alongside the rest rather than as an unexplained gap.
    COALESCE(density_class, -1) AS density_class,
    COUNT(*) AS cells,
    ROUND(MIN(shops_per_1k_residents), 3) AS min_value,
    ROUND(MAX(shops_per_1k_residents), 3) AS max_value,
    ROUND(APPROX_QUANTILES(shops_400m, 2)[OFFSET(1)], 1) AS median_shops_400m,
    ROUND(APPROX_QUANTILES(population_400m, 2)[OFFSET(1)], 0) AS median_population_400m
FROM report.shop_density
GROUP BY city, shop_type, density_class
