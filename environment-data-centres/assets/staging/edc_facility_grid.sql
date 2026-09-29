/* @bruin
name: staging.edc_facility_grid
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Assigns every facility in the spine to an eGRID subregion and attaches that
  subregion's annual average output emission rates.

  EPA publishes no county-to-subregion crosswalk, so assignment is by nearest
  generating plant from the eGRID plant file, restricted to plants with non-zero
  annual generation so retired or negligible units do not pull the assignment.
  A tie-break on generation prevents a tiny plant next door from overriding the
  region a site actually sits in.

  Limitation: nearest-plant assignment is a proxy for the balancing authority
  that actually serves the site. It is wrong near subregion boundaries. The
  distance to the assigned plant is retained so boundary cases can be filtered.

depends:
  - staging.edc_facility_spine
  - raw.edc_egrid_plants
  - raw.edc_egrid_subregion_rates

materialization:
  type: table
  strategy: create+replace

columns:
  - name: facility_key
    type: VARCHAR
    description: Facility key from the spine
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: name
    type: VARCHAR
    description: Facility name
  - name: operator
    type: VARCHAR
    description: Operator name
  - name: status
    type: VARCHAR
    description: Lifecycle status
  - name: state
    type: VARCHAR
    description: Two-letter US state code
  - name: county_fips
    type: VARCHAR
    description: Five-digit county FIPS
  - name: capacity_mw
    type: DOUBLE
    description: Best available capacity in MW, null where unknown
  - name: subregion_code
    type: VARCHAR
    description: Assigned eGRID subregion acronym
  - name: subregion_name
    type: VARCHAR
    description: Assigned eGRID subregion name
  - name: nearest_plant_km
    type: DOUBLE
    description: Distance to the nearest qualifying eGRID plant, in kilometres
  - name: co2e_rate_g_per_kwh
    type: DOUBLE
    description: Subregion annual average CO2-equivalent output rate in grams per kWh
  - name: nox_rate_lb_per_mwh
    type: DOUBLE
    description: Subregion annual average NOx output rate in pounds per MWh
  - name: coal_generation_pct
    type: DOUBLE
    description: Percentage of subregion generation from coal

@bruin */

WITH plants AS (
  SELECT
    subregion_code,
    ST_GEOGPOINT(lon, lat) AS geog,
    net_generation_mwh
  FROM `bruin-playground-arsalan.raw.edc_egrid_plants`
  WHERE lat IS NOT NULL
    AND lon IS NOT NULL
    AND subregion_code IS NOT NULL
    AND net_generation_mwh > 0
),

facilities AS (
  SELECT
    facility_key,
    name,
    operator,
    status,
    state,
    county_fips,
    capacity_mw,
    ST_GEOGPOINT(lon, lat) AS geog
  FROM `bruin-playground-arsalan.staging.edc_facility_spine`
  WHERE lat IS NOT NULL AND lon IS NOT NULL
),

/* Nearest qualifying plant per facility; larger generation wins an exact tie. */
assigned AS (
  SELECT
    f.facility_key,
    f.name,
    f.operator,
    f.status,
    f.state,
    f.county_fips,
    f.capacity_mw,
    p.subregion_code,
    ST_DISTANCE(f.geog, p.geog) / 1000 AS nearest_plant_km,
    ROW_NUMBER() OVER (
      PARTITION BY f.facility_key
      ORDER BY ST_DISTANCE(f.geog, p.geog), p.net_generation_mwh DESC
    ) AS rn
  FROM facilities f
  JOIN plants p
    ON ST_DWITHIN(f.geog, p.geog, 200000)
)

SELECT
  a.facility_key,
  a.name,
  a.operator,
  a.status,
  a.state,
  a.county_fips,
  a.capacity_mw,
  a.subregion_code,
  s.subregion_name,
  ROUND(a.nearest_plant_km, 2) AS nearest_plant_km,
  s.co2e_rate_g_per_kwh,
  s.nox_rate_lb_per_mwh,
  s.coal_generation_pct
FROM assigned a
LEFT JOIN `bruin-playground-arsalan.raw.edc_egrid_subregion_rates` s
  ON s.subregion_code = a.subregion_code
WHERE a.rn = 1
