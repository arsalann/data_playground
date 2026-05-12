/* @bruin

name: eu_pfas_staging.pf_nuts3_exposure
type: bq.sql
description: |
  NUTS3 PFAS exposure index aggregating site-level contamination data from the Forever
  Pollution Project and national monitoring systems across EU-27 countries. Each row
  represents a NUTS3 administrative region with summary statistics of PFAS contamination
  sites, water-matrix concentrations, and population-adjusted exposure metrics.

  This staging table serves as the primary analytical layer for regional PFAS exposure
  assessment, supporting compliance monitoring against EU Drinking Water Directive
  2020/2184 and source attribution analysis. Sites are spatially joined to NUTS3
  regions using nearest-centroid methodology with Eurostat GISCO label points.

  Key metrics per NUTS3:
    - Site counts: total sites and water-matrix samples within the region
    - Regulatory compliance: exceedances of 500 ng/L total PFAS threshold (EU DWD 2020/2184)
    - Concentration distributions: maximum and 95th percentile water-matrix values (ng/L)
    - Source attribution: counts by contamination source type (manufacturer, military, airport, industrial)
    - Population-adjusted densities: exposure metrics normalized per 100,000 inhabitants

  Water-matrix samples include drinking water, groundwater, and surface water sources
  relevant for human consumption pathways. Non-water matrices (soil, sediment, biota)
  are included in total site counts but excluded from regulatory threshold analysis.
connection: bruin-playground-arsalan
tags:
  - eu-27
  - pfas
  - staging
  - nuts3
  - exposure
  - environmental_health
  - regulatory_compliance
  - drinking_water
  - aggregated_fact_table
  - geospatial
  - population_adjusted
  - contamination_assessment
  - forever_chemicals

# Operational characteristics:
# - Refresh cadence: Weekly (aligned with pipeline schedule)
# - Data size: ~1,200 rows (one per EU-27 NUTS3 region)
# - Partitioning: None required due to small size
# - Growth pattern: Stable row count (fixed NUTS3 geography), values updated in-place
# - Performance: Fast aggregation over ~17K+ sites; table scans are efficient
# - Data quality: ~36% of regions have no water-matrix data (null PFAS concentrations)
# - Downstream usage: Primary source for DAC dashboard regional analysis and compliance reporting
# - Cross-pipeline dependency: Relies on shared NUTS3 dimension from eu-mortality pipeline

materialization:
  type: table
  strategy: create+replace

depends:
  - eu_pfas_staging.pf_sites_dim
  - eu_pfas_staging.pf_source_attribution

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: nuts_id
    type: VARCHAR
    description: NUTS3 administrative region identifier following Eurostat NUTS 2021 classification (5-character format, e.g., 'DE111'). Primary key with one row per region.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-2 country code (2 characters, e.g., 'DE' for Germany). Derived from NUTS3 prefix mapping.
    checks:
      - name: not_null
  - name: country_name_en
    type: VARCHAR
    description: Standardized English country name from Eurostat NUTS dimension (e.g., 'Germany'). Used for human-readable country identification.
    checks:
      - name: not_null
  - name: name_latn
    type: VARCHAR
    description: Official Latin/local name of the NUTS3 region from Eurostat GISCO dataset. May include special characters and local language variants.
    checks:
      - name: not_null
  - name: n_sites_total
    type: INTEGER
    description: Total count of all PFAS contamination sites spatially assigned to this NUTS3 region, including all sample matrices (water, soil, sediment, biota).
    checks:
      - name: not_null
      - name: positive
  - name: n_sites_water
    type: INTEGER
    description: Count of sites with water-matrix samples (drinking water, groundwater, surface water) relevant for regulatory compliance assessment. Subset of n_sites_total.
    checks:
      - name: not_null
      - name: positive
  - name: n_exceed_eu_dwd
    type: INTEGER
    description: Count of water-matrix samples exceeding 500 ng/L total PFAS concentration (EU Drinking Water Directive 2020/2184 threshold, enforceable from 12 January 2026). Critical regulatory compliance metric.
    checks:
      - name: not_null
      - name: positive
  - name: max_pfas_ng_l_water
    type: DOUBLE
    description: Maximum total PFAS concentration detected in water-matrix samples within this region (ng/L units). Null when no water-matrix data available (n_sites_water = 0).
  - name: p95_pfas_ng_l_water
    type: DOUBLE
    description: 95th percentile total PFAS concentration in water-matrix samples (ng/L). Provides distribution insight beyond maximum values. Null when insufficient water-matrix data.
  - name: n_sites_manufacturer
    type: INTEGER
    description: Count of contamination sites attributed to curated PFAS manufacturing facilities (3M, Solvay, Chemours, Daikin, Arkema) within 25km radius. Includes direct manufacturers and nearby facilities.
    checks:
      - name: not_null
      - name: positive
  - name: n_sites_military
    type: INTEGER
    description: Count of sites self-classified as military installations or firefighting training facilities where PFAS-containing firefighting foam was historically used.
    checks:
      - name: not_null
      - name: positive
  - name: n_sites_airport
    type: INTEGER
    description: Count of sites self-classified as airports where PFAS-containing firefighting foam was used for aviation safety protocols.
    checks:
      - name: not_null
      - name: positive
  - name: n_sites_industrial
    type: INTEGER
    description: Count of sites self-classified as industrial facilities or PFAS production plants, excluding dedicated PFAS manufacturers (counted separately in n_sites_manufacturer).
    checks:
      - name: not_null
      - name: positive
  - name: pop_total_2023
    type: DOUBLE
    description: Total population of the NUTS3 region in 2023 from Eurostat demographic data. Used as denominator for population-adjusted exposure density calculations.
    checks:
      - name: not_null
      - name: positive
  - name: water_site_density_per_100k
    type: DOUBLE
    description: Water-matrix sampling density calculated as (n_sites_water / pop_total_2023) × 100,000. Indicates monitoring intensity relative to population at risk.
    checks:
      - name: not_null
      - name: positive
  - name: exceedance_density_per_100k
    type: DOUBLE
    description: Regulatory exceedance density calculated as (n_exceed_eu_dwd / pop_total_2023) × 100,000. Key metric for population-adjusted PFAS exposure risk assessment.
    checks:
      - name: not_null
      - name: positive

@bruin */

WITH dim AS (
    SELECT * FROM `bruin-playground-arsalan.eu_mortality_staging.em_nuts3_dim`
),

sites AS (
    SELECT
        s.site_uid,
        s.nuts_id,
        s.is_water_sample,
        s.pfas_sum_ng_l,
        a.attribution_class
    FROM `bruin-playground-arsalan.eu_pfas_staging.pf_sites_dim` s
    LEFT JOIN `bruin-playground-arsalan.eu_pfas_staging.pf_source_attribution` a
        USING (site_uid)
    WHERE s.nuts_id IS NOT NULL
),

agg AS (
    SELECT
        nuts_id,
        COUNT(*) AS n_sites_total,
        COUNTIF(is_water_sample) AS n_sites_water,
        COUNTIF(is_water_sample AND pfas_sum_ng_l >= 500) AS n_exceed_eu_dwd,
        MAX(IF(is_water_sample, pfas_sum_ng_l, NULL)) AS max_pfas_ng_l_water,
        APPROX_QUANTILES(IF(is_water_sample, pfas_sum_ng_l, NULL),
                         100 IGNORE NULLS)[OFFSET(95)] AS p95_pfas_ng_l_water,
        COUNTIF(attribution_class = 'manufacturer'
                OR attribution_class = 'manufacturer_within_25km') AS n_sites_manufacturer,
        COUNTIF(attribution_class = 'military') AS n_sites_military,
        COUNTIF(attribution_class = 'airport') AS n_sites_airport,
        COUNTIF(attribution_class = 'industrial') AS n_sites_industrial
    FROM sites
    GROUP BY nuts_id
),

pop_2023 AS (
    SELECT nuts_id, pop_total AS pop_total_2023
    FROM `bruin-playground-arsalan.eu_mortality_staging.em_population_dim`
    WHERE ref_year = 2023
)

SELECT
    d.nuts_id,
    d.country_code,
    d.country_name_en,
    d.name_latn,
    CAST(IFNULL(a.n_sites_total, 0) AS INT64) AS n_sites_total,
    CAST(IFNULL(a.n_sites_water, 0) AS INT64) AS n_sites_water,
    CAST(IFNULL(a.n_exceed_eu_dwd, 0) AS INT64) AS n_exceed_eu_dwd,
    a.max_pfas_ng_l_water,
    a.p95_pfas_ng_l_water,
    CAST(IFNULL(a.n_sites_manufacturer, 0) AS INT64) AS n_sites_manufacturer,
    CAST(IFNULL(a.n_sites_military, 0) AS INT64) AS n_sites_military,
    CAST(IFNULL(a.n_sites_airport, 0) AS INT64) AS n_sites_airport,
    CAST(IFNULL(a.n_sites_industrial, 0) AS INT64) AS n_sites_industrial,
    p.pop_total_2023,
    SAFE_DIVIDE(IFNULL(a.n_sites_water, 0), p.pop_total_2023) * 100000 AS water_site_density_per_100k,
    SAFE_DIVIDE(IFNULL(a.n_exceed_eu_dwd, 0), p.pop_total_2023) * 100000 AS exceedance_density_per_100k
FROM dim d
LEFT JOIN agg a USING (nuts_id)
LEFT JOIN pop_2023 p USING (nuts_id)
ORDER BY d.nuts_id
