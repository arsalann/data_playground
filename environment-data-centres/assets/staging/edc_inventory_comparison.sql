/* @bruin
name: staging.edc_inventory_comparison
type: bq.sql
connection: bruin-playground-arsalan
description: |
  How many US data centres are there? Every public inventory gives a different
  answer, and the disagreement is the finding.

  Two things are measured here:

  1. Headline counts per inventory, with the counting unit each one uses.
     Commercial listing sites are included as cited published figures, not as
     ingested records, because they count colocation suites rather than sites.

  2. Overlap sensitivity: how many Compute Atlas facilities have a FracTracker
     record within a given radius. The overlap is not a fixed number - it is a
     function of the match radius, which is precisely why counts disagree.
     250 m is roughly one building, 1 km one campus, 20 km a metro area.

depends:
  - raw.edc_compute_atlas_facilities
  - raw.edc_fractracker_facilities
  - raw.edc_osm_data_centres
  - staging.edc_facility_spine

materialization:
  type: table
  strategy: create+replace

columns:
  - name: metric_type
    type: VARCHAR
    description: "Either inventory_count or overlap_sensitivity"
    primary_key: true
  - name: label
    type: VARCHAR
    description: Inventory name, or the match radius for overlap rows
    primary_key: true
  - name: sort_order
    type: INTEGER
    description: Display order, ascending
  - name: facility_count
    type: INTEGER
    description: Number of records the inventory publishes, or matched facilities for overlap rows
  - name: pct_of_compute_atlas
    type: DOUBLE
    description: For overlap rows, matched facilities as a percentage of the 937 Compute Atlas records
  - name: counting_unit
    type: VARCHAR
    description: What one record represents in that inventory
  - name: is_ingested
    type: BOOLEAN
    description: True if this pipeline ingests the records, false if the figure is cited from the publisher
  - name: source_note
    type: VARCHAR
    description: Provenance and licence note

@bruin */

WITH inventory_counts AS (
  SELECT
    'Compute Atlas' AS label,
    1 AS sort_order,
    (SELECT COUNT(*) FROM `bruin-playground-arsalan.raw.edc_compute_atlas_facilities`) AS facility_count,
    'Site, source-cited' AS counting_unit,
    TRUE AS is_ingested,
    'compute-atlas.com public JSON API, CC-BY-4.0' AS source_note

  UNION ALL
  SELECT
    'FracTracker', 2,
    (SELECT COUNT(*) FROM `bruin-playground-arsalan.raw.edc_fractracker_facilities`),
    'Site, includes proposed and cancelled',
    TRUE,
    'FracTracker Alliance ArcGIS FeatureServer, free for non-commercial use'

  UNION ALL
  SELECT
    'OpenStreetMap', 3,
    (SELECT COUNT(*) FROM `bruin-playground-arsalan.raw.edc_osm_data_centres`),
    'Tagged feature (node, way or relation)',
    TRUE,
    'Overpass API, telecom=data_center and building=data_center, ODbL'

  UNION ALL
  SELECT
    'Deduplicated spine', 4,
    (SELECT COUNT(*) FROM `bruin-playground-arsalan.staging.edc_facility_spine`),
    'Site, Compute Atlas and FracTracker merged',
    TRUE,
    'This pipeline: 1 km proximity plus shared operator token, or 250 m regardless'

  UNION ALL
  SELECT
    'DataCenterMap', 5, 4767,
    'Listing, includes colocation suites',
    FALSE,
    'Published figure from datacentermap.com/usa, not ingested'

  UNION ALL
  SELECT
    'Baxtel', 6, 5115,
    'Listing, includes colocation suites',
    FALSE,
    'Published figure from baxtel.com, not ingested'
),

/* Compute Atlas facilities that have at least one FracTracker record within each radius. */
radii AS (
  SELECT * FROM UNNEST([250, 500, 1000, 2000, 5000, 10000, 20000]) AS radius_m
),

ca AS (
  SELECT facility_id, ST_GEOGPOINT(lon, lat) AS geog
  FROM `bruin-playground-arsalan.raw.edc_compute_atlas_facilities`
  WHERE lat IS NOT NULL AND lon IS NOT NULL
),

ft AS (
  SELECT facility_id, ST_GEOGPOINT(lon, lat) AS geog
  FROM `bruin-playground-arsalan.raw.edc_fractracker_facilities`
  WHERE lat IS NOT NULL AND lon IS NOT NULL
),

ca_total AS (
  SELECT COUNT(*) AS n FROM ca
),

overlap AS (
  SELECT
    r.radius_m,
    COUNT(DISTINCT ca.facility_id) AS matched
  FROM radii r
  CROSS JOIN ca
  JOIN ft ON ST_DWITHIN(ca.geog, ft.geog, r.radius_m)
  GROUP BY r.radius_m
)

SELECT
  'inventory_count' AS metric_type,
  label,
  sort_order,
  facility_count,
  CAST(NULL AS FLOAT64) AS pct_of_compute_atlas,
  counting_unit,
  is_ingested,
  source_note
FROM inventory_counts

UNION ALL

SELECT
  'overlap_sensitivity',
  CASE
    WHEN o.radius_m < 1000 THEN CONCAT(CAST(o.radius_m AS STRING), ' m')
    ELSE CONCAT(CAST(o.radius_m / 1000 AS STRING), ' km')
  END,
  o.radius_m,
  o.matched,
  ROUND(100 * o.matched / t.n, 1),
  CASE
    WHEN o.radius_m <= 500 THEN 'Roughly one building'
    WHEN o.radius_m <= 2000 THEN 'Roughly one campus'
    WHEN o.radius_m <= 5000 THEN 'Neighbouring sites'
    ELSE 'Metro area, not a site match'
  END,
  TRUE,
  'Compute Atlas facilities with any FracTracker record within the radius'
FROM overlap o
CROSS JOIN ca_total t

ORDER BY metric_type, sort_order
