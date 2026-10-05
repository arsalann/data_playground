/* @bruin
name: staging.edc_facility_spine
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Unified US data centre facility spine built from Compute Atlas and FracTracker,
  deduplicated spatially. Every downstream hypothesis joins to this table.

  Dedup method: two records match when they are within 1 km of each other AND
  their operator names share a normalised token prefix, OR within 250 m
  regardless of operator (same site, different naming). Compute Atlas is the
  preferred record where both sources describe the same site, because every
  Compute Atlas record is source-cited; FracTracker fields fill the gaps.

  Deliberately NOT merged in: OpenStreetMap (geometry only, no attributes) and
  the commercial listing sites (colocation suites inflate counts). Both are
  compared against this spine in staging.edc_inventory_comparison.

depends:
  - raw.edc_compute_atlas_facilities
  - raw.edc_fractracker_facilities
  - raw.edc_county_reference

materialization:
  type: table
  strategy: create+replace

columns:
  - name: facility_key
    type: VARCHAR
    description: Stable synthetic key for the deduplicated facility
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: name
    type: VARCHAR
    description: Preferred facility name
  - name: operator
    type: VARCHAR
    description: Preferred operator name
  - name: status
    type: VARCHAR
    description: "Normalised lifecycle status: operational, under_construction, permitted, proposed, cancelled"
  - name: lat
    type: DOUBLE
    description: Latitude in WGS84 decimal degrees
  - name: lon
    type: DOUBLE
    description: Longitude in WGS84 decimal degrees
  - name: state
    type: VARCHAR
    description: Two-letter US state code
  - name: county_name
    type: VARCHAR
    description: County name as published by the source inventory
  - name: county_fips
    type: VARCHAR
    description: Five-digit county FIPS resolved via the Census gazetteer, null where unmatched
  - name: capacity_mw
    type: DOUBLE
    description: Best available capacity in MW; Compute Atlas planned or operational, else the FracTracker range midpoint
  - name: capacity_source
    type: VARCHAR
    description: Which inventory supplied capacity_mw
  - name: cooling_type
    type: VARCHAR
    description: Cooling technology, coalesced across sources, null where neither reports it
  - name: cooling_source_inventory
    type: VARCHAR
    description: Which inventory supplied cooling_type
  - name: land_acres
    type: DOUBLE
    description: Site area in acres where reported
  - name: in_compute_atlas
    type: BOOLEAN
    description: True if a Compute Atlas record contributed to this facility
  - name: in_fractracker
    type: BOOLEAN
    description: True if a FracTracker record contributed to this facility
  - name: source_count
    type: INTEGER
    description: Number of cited public sources on the Compute Atlas record, null for FracTracker-only facilities
  - name: status_history_count
    type: INTEGER
    description: Number of recorded status transitions, null for FracTracker-only facilities
  - name: location_precision
    type: VARCHAR
    description: Coordinate precision where the source reports it

@bruin */

WITH ca AS (
  SELECT
    facility_id,
    name,
    operator,
    status,
    lat,
    lon,
    UPPER(TRIM(state)) AS state,
    county AS county_name,
    COALESCE(capacity_mw_operational, capacity_mw_planned) AS capacity_mw,
    cooling_type,
    land_acres,
    source_count,
    status_history_count,
    location_precision
  FROM `bruin-playground-arsalan.raw.edc_compute_atlas_facilities`
  WHERE lat IS NOT NULL AND lon IS NOT NULL
),

ft AS (
  SELECT
    facility_id,
    facility_name AS name,
    operator_name AS operator,
    -- FracTracker publishes free-form status labels; map them onto the Compute Atlas vocabulary.
    CASE
      WHEN LOWER(status) LIKE '%operating%' OR LOWER(status) LIKE '%operational%' THEN 'operational'
      WHEN LOWER(status) LIKE '%construction%' THEN 'under_construction'
      WHEN LOWER(status) LIKE '%approved%' OR LOWER(status) LIKE '%permitted%' THEN 'permitted'
      WHEN LOWER(status) LIKE '%propos%' THEN 'proposed'
      WHEN LOWER(status) LIKE '%cancel%' THEN 'cancelled'
      WHEN LOWER(status) LIKE '%expand%' THEN 'operational'
      WHEN LOWER(status) LIKE '%suspend%' THEN 'suspended'
      ELSE LOWER(NULLIF(TRIM(status), ''))
    END AS status,
    lat,
    lon,
    UPPER(TRIM(state)) AS state,
    county AS county_name,
    -- Midpoint of the reported range, or whichever bound is present.
    CASE
      WHEN mw_low IS NOT NULL AND mw_high IS NOT NULL THEN (mw_low + mw_high) / 2
      ELSE COALESCE(mw_high, mw_low)
    END AS capacity_mw,
    NULLIF(TRIM(cooling_type), '') AS cooling_type,
    SAFE_CAST(NULLIF(REGEXP_REPLACE(property_size_acres, r'[^0-9.]', ''), '') AS FLOAT64) AS land_acres,
    location_confidence AS location_precision
  FROM `bruin-playground-arsalan.raw.edc_fractracker_facilities`
  WHERE lat IS NOT NULL AND lon IS NOT NULL
),

/* Candidate matches between the two inventories. A FracTracker record matches a
   Compute Atlas record when they are very close, or close with a shared operator
   token. Ranked so each FracTracker record claims at most one Compute Atlas record. */
matched AS (
  SELECT
    ft.facility_id AS ft_id,
    ca.facility_id AS ca_id,
    ROW_NUMBER() OVER (
      PARTITION BY ft.facility_id
      ORDER BY ST_DISTANCE(ST_GEOGPOINT(ft.lon, ft.lat), ST_GEOGPOINT(ca.lon, ca.lat))
    ) AS rn
  FROM ft
  JOIN ca
    ON ST_DWITHIN(ST_GEOGPOINT(ft.lon, ft.lat), ST_GEOGPOINT(ca.lon, ca.lat), 1000)
   AND (
        ST_DWITHIN(ST_GEOGPOINT(ft.lon, ft.lat), ST_GEOGPOINT(ca.lon, ca.lat), 250)
        OR LOWER(SPLIT(TRIM(ft.operator), ' ')[SAFE_OFFSET(0)])
           = LOWER(SPLIT(TRIM(ca.operator), ' ')[SAFE_OFFSET(0)])
       )
),

ft_to_ca AS (
  SELECT ft_id, ca_id FROM matched WHERE rn = 1
),

/* One row per facility: Compute Atlas record plus any FracTracker fields that fill gaps. */
merged AS (
  SELECT
    CONCAT('ca:', ca.facility_id) AS facility_key,
    COALESCE(ca.name, ANY_VALUE(ft.name)) AS name,
    COALESCE(ca.operator, ANY_VALUE(ft.operator)) AS operator,
    COALESCE(ca.status, ANY_VALUE(ft.status)) AS status,
    ca.lat,
    ca.lon,
    COALESCE(ca.state, ANY_VALUE(ft.state)) AS state,
    COALESCE(ca.county_name, ANY_VALUE(ft.county_name)) AS county_name,
    COALESCE(ca.capacity_mw, ANY_VALUE(ft.capacity_mw)) AS capacity_mw,
    CASE
      WHEN ca.capacity_mw IS NOT NULL THEN 'compute_atlas'
      WHEN ANY_VALUE(ft.capacity_mw) IS NOT NULL THEN 'fractracker'
    END AS capacity_source,
    COALESCE(ca.cooling_type, ANY_VALUE(ft.cooling_type)) AS cooling_type,
    CASE
      WHEN ca.cooling_type IS NOT NULL THEN 'compute_atlas'
      WHEN ANY_VALUE(ft.cooling_type) IS NOT NULL THEN 'fractracker'
    END AS cooling_source_inventory,
    COALESCE(ca.land_acres, ANY_VALUE(ft.land_acres)) AS land_acres,
    TRUE AS in_compute_atlas,
    COUNTIF(ft.facility_id IS NOT NULL) > 0 AS in_fractracker,
    ca.source_count,
    ca.status_history_count,
    ca.location_precision
  FROM ca
  LEFT JOIN ft_to_ca ON ft_to_ca.ca_id = ca.facility_id
  LEFT JOIN ft ON ft.facility_id = ft_to_ca.ft_id
  GROUP BY
    ca.facility_id, ca.name, ca.operator, ca.status, ca.lat, ca.lon, ca.state,
    ca.county_name, ca.capacity_mw, ca.cooling_type, ca.land_acres,
    ca.source_count, ca.status_history_count, ca.location_precision
),

/* FracTracker records with no Compute Atlas counterpart become their own facilities. */
ft_only AS (
  SELECT
    CONCAT('ft:', ft.facility_id) AS facility_key,
    ft.name,
    ft.operator,
    ft.status,
    ft.lat,
    ft.lon,
    ft.state,
    ft.county_name,
    ft.capacity_mw,
    CASE WHEN ft.capacity_mw IS NOT NULL THEN 'fractracker' END AS capacity_source,
    ft.cooling_type,
    CASE WHEN ft.cooling_type IS NOT NULL THEN 'fractracker' END AS cooling_source_inventory,
    ft.land_acres,
    FALSE AS in_compute_atlas,
    TRUE AS in_fractracker,
    CAST(NULL AS INT64) AS source_count,
    CAST(NULL AS INT64) AS status_history_count,
    ft.location_precision
  FROM ft
  LEFT JOIN ft_to_ca ON ft_to_ca.ft_id = ft.facility_id
  WHERE ft_to_ca.ft_id IS NULL
),

combined AS (
  SELECT * FROM merged
  UNION ALL
  SELECT * FROM ft_only
)

SELECT
  c.facility_key,
  c.name,
  c.operator,
  c.status,
  c.lat,
  c.lon,
  c.state,
  c.county_name,
  ref.county_fips,
  c.capacity_mw,
  c.capacity_source,
  c.cooling_type,
  c.cooling_source_inventory,
  c.land_acres,
  c.in_compute_atlas,
  c.in_fractracker,
  c.source_count,
  c.status_history_count,
  c.location_precision
FROM combined c
LEFT JOIN `bruin-playground-arsalan.raw.edc_county_reference` ref
  ON ref.state = c.state
 AND ref.county_name_norm = REGEXP_REPLACE(LOWER(TRIM(c.county_name)), r"[.'`]", '')
