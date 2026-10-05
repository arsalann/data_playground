/* @bruin
name: report.osm_register_gap
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Quantifies what an OpenStreetMap-only method misses, by comparing OSM objects to Madrid's
  official register over the same bounding box and the same shop types.

  This is the empirical justification for the whole project. The reference project ranked
  candidate cafe sites in Istanbul from 744 OSM cafes with no way to know whether that was
  most of the cafes or half of them. Madrid is the right place to answer that question because
  its register is the strongest in the shortlist, so the register can act as the reference
  universe rather than being just another estimate.

  Two measurements, because the gap is not only a matter of count:

  1. `coverage_ratio` compares raw counts. Below 1 means OSM has fewer objects than the
     register, above 1 means more.
  2. `osm_matched_pct` asks whether OSM's objects are actually the register's objects, by
     testing for a same-type register premise within 75 m. A low match rate alongside a
     coverage ratio near 1 would mean OSM has a similar number of different things, which is
     a worse failure than a simple undercount.

  The 75 m threshold is deliberately loose. OSM nodes are placed by eye and the register
  geocodes to the building entrance, so the two disagree by tens of metres on the same
  premise. A tighter threshold would measure geocoding precision rather than presence.

  Both sides are restricted to the identical Madrid municipal bounding box used by every other
  asset in this pipeline, so the comparison is not confounded by area.

depends:
  - raw.osm_madrid_amenities
  - staging.shops_unified

materialization:
  type: table
  strategy: create+replace

columns:
  - name: shop_type
    type: VARCHAR
    description: Canonical shop type being compared.
    primary_key: true
    nullable: false
  - name: register_count
    type: INTEGER
    description: Open, geocoded establishments of this type in the Madrid municipal register.
  - name: osm_count
    type: INTEGER
    description: OpenStreetMap objects of this type in the same bounding box.
  - name: coverage_ratio
    type: DOUBLE
    description: osm_count divided by register_count. Below 1 means OpenStreetMap has fewer objects than the register.
  - name: osm_matched
    type: INTEGER
    description: OpenStreetMap objects with a same-type register premise within 75 m.
  - name: osm_matched_pct
    type: DOUBLE
    description: Share of OpenStreetMap objects that match a register premise, as a percentage.
  - name: osm_unmatched
    type: INTEGER
    description: OpenStreetMap objects with no same-type register premise within 75 m.
  - name: register_missing_from_osm
    type: INTEGER
    description: Register premises with no same-type OpenStreetMap object within 75 m. These are the premises an OpenStreetMap-only study would treat as absent.
  - name: register_missing_pct
    type: DOUBLE
    description: Share of register premises absent from OpenStreetMap, as a percentage. This is the headline number.
  - name: interpretation
    type: VARCHAR
    description: Plain-language reading of the gap for this shop type.

@bruin */

WITH register AS (
    SELECT
        shop_type,
        establishment_id,
        ST_GEOGPOINT(ANY_VALUE(lon), ANY_VALUE(lat)) AS point
    FROM staging.shops_unified
    WHERE city = 'madrid' AND is_open AND is_geocoded AND is_usable
    GROUP BY shop_type, establishment_id
),

osm AS (
    SELECT
        shop_type,
        osm_type,
        osm_id,
        ST_GEOGPOINT(ANY_VALUE(lon), ANY_VALUE(lat)) AS point
    FROM raw.osm_madrid_amenities
    GROUP BY shop_type, osm_type, osm_id
),

osm_matching AS (
    SELECT
        o.shop_type,
        o.osm_type,
        o.osm_id,
        LOGICAL_OR(r.establishment_id IS NOT NULL) AS is_matched
    FROM osm o
    LEFT JOIN register r
        ON r.shop_type = o.shop_type AND ST_DWITHIN(o.point, r.point, 75)
    GROUP BY o.shop_type, o.osm_type, o.osm_id
),

register_matching AS (
    SELECT
        r.shop_type,
        r.establishment_id,
        LOGICAL_OR(o.osm_id IS NOT NULL) AS is_matched
    FROM register r
    LEFT JOIN osm o
        ON o.shop_type = r.shop_type AND ST_DWITHIN(r.point, o.point, 75)
    GROUP BY r.shop_type, r.establishment_id
),

aggregated AS (
    SELECT
        COALESCE(rm.shop_type, om.shop_type) AS shop_type,
        COALESCE(rm.register_count, 0) AS register_count,
        COALESCE(om.osm_count, 0) AS osm_count,
        COALESCE(om.osm_matched, 0) AS osm_matched,
        COALESCE(rm.register_matched, 0) AS register_matched
    FROM (
        SELECT
            shop_type,
            COUNT(*) AS register_count,
            COUNTIF(is_matched) AS register_matched
        FROM register_matching
        GROUP BY shop_type
    ) rm
    FULL OUTER JOIN (
        SELECT
            shop_type,
            COUNT(*) AS osm_count,
            COUNTIF(is_matched) AS osm_matched
        FROM osm_matching
        GROUP BY shop_type
    ) om USING (shop_type)
)

SELECT
    shop_type,
    register_count,
    osm_count,
    ROUND(SAFE_DIVIDE(osm_count, register_count), 3) AS coverage_ratio,
    osm_matched,
    ROUND(SAFE_DIVIDE(100.0 * osm_matched, osm_count), 1) AS osm_matched_pct,
    osm_count - osm_matched AS osm_unmatched,
    register_count - register_matched AS register_missing_from_osm,
    ROUND(SAFE_DIVIDE(100.0 * (register_count - register_matched), register_count), 1)
        AS register_missing_pct,
    CASE
        WHEN SAFE_DIVIDE(osm_count, register_count) < 0.5
            THEN 'OpenStreetMap holds under half the registered premises. A competition surface built from it would read large parts of the city as underserved when they are not.'
        WHEN SAFE_DIVIDE(osm_count, register_count) < 0.85
            THEN 'OpenStreetMap is materially incomplete. Site rankings built from it would be biased towards poorly-mapped districts.'
        WHEN SAFE_DIVIDE(osm_count, register_count) <= 1.15
            THEN 'Counts are close, but check the match rate: a similar total made of different objects is not the same universe.'
        ELSE 'OpenStreetMap holds more objects than the register class. The two are not measuring the same thing, because the OpenStreetMap tag is broader than the licence class.'
    END AS interpretation
FROM aggregated
ORDER BY register_count DESC
