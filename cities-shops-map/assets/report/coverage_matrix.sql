/* @bruin
name: report.coverage_matrix
type: bq.sql
connection: bruin-playground-arsalan
description: |
  The city by shop-type data-quality matrix, one row per combination, joining the rubric
  sub-scores to the counts actually observed in the register and to the caveat text.

  This is the first thing the map shows, before any density figure, because a shop count per
  1,000 residents is only as meaningful as the universe it counts. Every combination carries
  its own caveats rather than the reader being handed one global disclaimer.

  `is_mappable` applies two rules: the shop type must be separable in that city's taxonomy,
  and there must be at least 50 geocoded open establishments. The threshold exists because a
  density surface built from a couple of dozen points describes those points rather than the
  city. Chicago bookstores fail it with 24, which is the honest outcome: the activity token
  exists but most Chicago book retailers hold a licence with no activity token at all.

depends:
  - staging.shops_unified
  - staging.shop_type_crosswalk
  - staging.data_quality_scores

materialization:
  type: table
  strategy: create+replace

columns:
  - name: city
    type: VARCHAR
    description: City slug.
    primary_key: true
    nullable: false
  - name: city_label
    type: VARCHAR
    description: Display name of the city.
  - name: shop_type
    type: VARCHAR
    description: Canonical shop type.
    primary_key: true
    nullable: false
  - name: shop_type_label
    type: VARCHAR
    description: Display name of the shop type.
  - name: score
    type: DOUBLE
    description: Weighted data-quality score out of 100 for this city and shop type.
  - name: authority
    type: INTEGER
    description: Source authority sub-score, 0-10.
  - name: coverage
    type: INTEGER
    description: Coverage sub-score for this shop type, 0-10.
  - name: granularity
    type: INTEGER
    description: Category granularity sub-score, 0-10.
  - name: freshness
    type: INTEGER
    description: Freshness sub-score, 0-10.
  - name: geocoding
    type: INTEGER
    description: Geocoding sub-score, 0-10.
  - name: access
    type: INTEGER
    description: Access and licence sub-score, 0-10.
  - name: establishments_open
    type: INTEGER
    description: Distinct open establishments of this type in the register.
  - name: establishments_mappable
    type: INTEGER
    description: Distinct open establishments that also carry coordinates inside the city bounding box, so can appear on the map.
  - name: pct_geocoded
    type: DOUBLE
    description: Share of open establishments of this type that carry usable coordinates, as a percentage.
  - name: native_code_count
    type: INTEGER
    description: Number of distinct source activity codes mapped to this shop type.
  - name: separability
    type: VARCHAR
    description: Most pessimistic separability grade across the codes mapped to this type.
  - name: is_usable
    type: BOOLEAN
    description: False where the shop type cannot be separated in this city's taxonomy at all.
  - name: is_mappable
    type: BOOLEAN
    description: True where the type is separable and has at least 50 mappable open establishments, so a density surface describes the city rather than a handful of points.
  - name: source_name
    type: VARCHAR
    description: Publisher and dataset behind this combination.
  - name: source_vintage
    type: VARCHAR
    description: Vintage of the source data.
  - name: native_codes
    type: VARCHAR
    description: Comma-separated list of source codes mapped to this shop type, so the classification is auditable from the map.
  - name: caveats
    type: VARCHAR
    description: Concatenated caveat text for every code mapped to this type, for reading in a SQL client.
  - name: caveats_json
    type: VARCHAR
    description: The same caveats as a JSON array of one string per source code. The map reads this, so it never has to split a concatenated string back apart.

@bruin */

WITH city_labels AS (
    SELECT * FROM UNNEST([
        STRUCT('madrid' AS city, 'Madrid' AS city_label),
        ('paris', 'Paris'),
        ('mexico_city', 'Mexico City'),
        ('london', 'London'),
        ('chicago', 'Chicago')
    ])
),

shop_type_labels AS (
    SELECT * FROM UNNEST([
        STRUCT('bar_pub' AS shop_type, 'Bar / pub' AS shop_type_label),
        ('nightclub', 'Nightclub'),
        ('cafe', 'Cafe'),
        ('restaurant', 'Restaurant'),
        ('fast_food', 'Fast food / takeaway'),
        ('bakery', 'Bakery / pastry'),
        ('bookstore', 'Bookstore')
    ])
),

observed AS (
    SELECT
        city,
        shop_type,
        COUNT(DISTINCT IF(is_open, establishment_id, NULL)) AS establishments_open,
        COUNT(DISTINCT IF(is_open AND is_geocoded, establishment_id, NULL)) AS establishments_mappable,
        MIN(separability) AS separability,
        LOGICAL_AND(is_usable) AS is_usable,
        ANY_VALUE(source_name) AS source_name,
        ANY_VALUE(source_vintage) AS source_vintage
    FROM staging.shops_unified
    GROUP BY city, shop_type
),

codes AS (
    SELECT
        city,
        shop_type,
        COUNT(*) AS native_code_count,
        STRING_AGG(native_code, ', ' ORDER BY native_code) AS native_codes,
        STRING_AGG(CONCAT(native_code, ': ', caveat), ' ' ORDER BY native_code) AS caveats,
        TO_JSON_STRING(
            ARRAY_AGG(CONCAT(native_code, ': ', caveat) ORDER BY native_code)
        ) AS caveats_json
    FROM staging.shop_type_crosswalk
    GROUP BY city, shop_type
)

SELECT
    q.city,
    cl.city_label,
    q.shop_type,
    sl.shop_type_label,
    q.score,
    q.authority,
    q.coverage,
    q.granularity,
    q.freshness,
    q.geocoding,
    q.access,
    COALESCE(o.establishments_open, 0) AS establishments_open,
    COALESCE(o.establishments_mappable, 0) AS establishments_mappable,
    ROUND(
        SAFE_DIVIDE(100.0 * o.establishments_mappable, o.establishments_open), 1
    ) AS pct_geocoded,
    COALESCE(c.native_code_count, 0) AS native_code_count,
    COALESCE(o.separability, 'no_mapping') AS separability,
    COALESCE(o.is_usable, FALSE) AS is_usable,
    COALESCE(o.is_usable, FALSE) AND COALESCE(o.establishments_mappable, 0) >= 50
        AS is_mappable,
    o.source_name,
    o.source_vintage,
    c.native_codes,
    c.caveats,
    c.caveats_json
FROM staging.data_quality_scores q
JOIN city_labels cl USING (city)
JOIN shop_type_labels sl USING (shop_type)
LEFT JOIN observed o ON o.city = q.city AND o.shop_type = q.shop_type
LEFT JOIN codes c ON c.city = q.city AND c.shop_type = q.shop_type
ORDER BY q.city, q.score DESC
