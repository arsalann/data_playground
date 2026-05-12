/* @bruin

name: eu_pfas_staging.pf_nuts3_exposure
type: bq.sql
description: |
  NUTS3 PFAS exposure index built by rolling site-level measurements up to the
  NUTS3 region the site was assigned to.

  Per NUTS3 row:
    - n_sites_total, n_sites_water: site counts.
    - n_exceed_eu_dwd: water-matrix samples >= 500 ng/L (EU Drinking Water
      Directive 2020/2184 threshold for total PFAS, in force from 12 January
      2026).
    - max_pfas_ng_l_water, p95_pfas_ng_l_water: distribution of water-matrix
      concentrations.
    - n_sites_manufacturer, n_sites_military, n_sites_airport, n_sites_industrial:
      counts by attribution class.
    - water_site_density_per_100k: drinking-water sample density per 100,000
      inhabitants.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - eu_pfas_staging.pf_sites_dim
  - eu_pfas_staging.pf_source_attribution
  # Cross-pipeline (eu-mortality):
  #   eu_mortality_staging.em_nuts3_dim - NUTS3 dimension anchor.
  #   eu_mortality_staging.em_population_dim - 2023 population denominator.

tags:
  - eu-27
  - pfas
  - staging
  - nuts3
  - exposure

columns:
  - name: nuts_id
    type: VARCHAR
    description: NUTS3 region code.
    primary_key: true
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-2.
  - name: country_name_en
    type: VARCHAR
    description: English country name.
  - name: name_latn
    type: VARCHAR
    description: Latin regional name.
  - name: n_sites_total
    type: INTEGER
    description: PFAS sites assigned to the region.
  - name: n_sites_water
    type: INTEGER
    description: Water-matrix sample count.
  - name: n_exceed_eu_dwd
    type: INTEGER
    description: Water-matrix samples >= 500 ng/L.
  - name: max_pfas_ng_l_water
    type: DOUBLE
    description: Max water-matrix concentration (ng/L).
  - name: p95_pfas_ng_l_water
    type: DOUBLE
    description: 95th percentile water-matrix concentration (ng/L).
  - name: n_sites_manufacturer
    type: INTEGER
    description: Sites attributed to a curated PFAS manufacturer.
  - name: n_sites_military
    type: INTEGER
    description: Sites self-classified as military / firefighting.
  - name: n_sites_airport
    type: INTEGER
    description: Sites self-classified as airport.
  - name: n_sites_industrial
    type: INTEGER
    description: Sites self-classified as industrial.
  - name: pop_total_2023
    type: DOUBLE
    description: NUTS3 total population in 2023.
  - name: water_site_density_per_100k
    type: DOUBLE
    description: n_sites_water / pop_total_2023 * 100,000.
  - name: exceedance_density_per_100k
    type: DOUBLE
    description: n_exceed_eu_dwd / pop_total_2023 * 100,000.

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
