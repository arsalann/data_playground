/* @bruin

name: eu_env_dashboard_report.env_burden_nuts3
type: bq.sql
description: |
  Composite environmental burden index per NUTS3 region in the EU-27.

  Joins per-NUTS3:
    - Heat mortality (rolled up from eu_mortality_staging.em_heat_attribution).
    - PFAS exposure (from eu_pfas_staging.pf_nuts3_exposure).
    - Vulnerability dimensions (DEGURBA, coast, mountain, age-share).
    - NUTS3 metadata (name, country, centroid).

  Composite index z-scores each component across the EU-27 NUTS3 panel and
  averages standardised values for ranking. Components are stored as raw values
  too so the dashboard can show decomposition.

  Two ranking variants:
    - burden_idx_heat_pfas: mean z(heat excess per 100k) + z(exceedance density)
    - burden_idx_pfas_only: z(exceedance density) (used when temperature data is
      incomplete -- the current scope ingests temperature for 3 demonstration
      NUTS3 only, so heat-attribution z-scores are 0 for the bulk of regions).
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace


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
  - name: degurba_label
    type: VARCHAR
    description: DEGURBA classification.
  - name: coast_label
    type: VARCHAR
    description: Coastal class.
  - name: centroid_lat
    type: DOUBLE
    description: Representative latitude.
  - name: centroid_lon
    type: DOUBLE
    description: Representative longitude.
  - name: excess_deaths_total
    type: DOUBLE
    description: Sum of weekly excess deaths 2015-2025 (any direction).
  - name: heat_deaths_per_100k
    type: DOUBLE
    description: Heat-attributable excess deaths per 100,000 inhabitants.
  - name: avg_pop_total
    type: DOUBLE
    description: Mean population across the window.
  - name: avg_share_65plus
    type: DOUBLE
    description: Mean share of population aged 65+.
  - name: n_sites_water
    type: INTEGER
    description: Water-matrix PFAS samples assigned to the region.
  - name: n_exceed_eu_dwd
    type: INTEGER
    description: Water samples >= 500 ng/L.
  - name: exceedance_density_per_100k
    type: DOUBLE
    description: n_exceed_eu_dwd / population * 100,000.
  - name: max_pfas_ng_l_water
    type: DOUBLE
    description: Maximum water-matrix PFAS in the region (ng/L).
  - name: n_sites_manufacturer
    type: INTEGER
    description: Sites near (within 5 km of) a curated PFAS manufacturer.
  - name: n_sites_military
    type: INTEGER
    description: Sites self-classified as military / firefighting.
  - name: burden_idx_pfas_only
    type: DOUBLE
    description: Z-scored PFAS exceedance density (centred and scaled across the EU-27 panel).
  - name: burden_idx_heat_pfas
    type: DOUBLE
    description: Mean of z(heat_deaths_per_100k) and z(exceedance_density_per_100k).

@bruin */

WITH dim AS (
    SELECT * FROM `bruin-playground-arsalan.eu_mortality_staging.em_nuts3_dim`
),

heat AS (
    SELECT * FROM `bruin-playground-arsalan.eu_mortality_report.em_top_nuts3_heat_deaths`
),

pfas AS (
    SELECT * FROM `bruin-playground-arsalan.eu_pfas_staging.pf_nuts3_exposure`
),

joined AS (
    SELECT
        d.nuts_id,
        d.country_code,
        d.country_name_en,
        d.name_latn,
        d.degurba_label,
        d.coast_label,
        d.centroid_lat,
        d.centroid_lon,
        IFNULL(h.excess_deaths_total, 0) AS excess_deaths_total,
        IFNULL(h.heat_deaths_per_100k, 0) AS heat_deaths_per_100k,
        h.avg_pop_total,
        h.avg_share_65plus,
        IFNULL(p.n_sites_water, 0) AS n_sites_water,
        IFNULL(p.n_exceed_eu_dwd, 0) AS n_exceed_eu_dwd,
        IFNULL(p.exceedance_density_per_100k, 0) AS exceedance_density_per_100k,
        p.max_pfas_ng_l_water,
        IFNULL(p.n_sites_manufacturer, 0) AS n_sites_manufacturer,
        IFNULL(p.n_sites_military, 0) AS n_sites_military
    FROM dim d
    LEFT JOIN heat h USING (nuts_id)
    LEFT JOIN pfas p USING (nuts_id)
),

stats AS (
    SELECT
        AVG(exceedance_density_per_100k) AS m_excd,
        STDDEV(exceedance_density_per_100k) AS s_excd,
        AVG(heat_deaths_per_100k) AS m_heat,
        STDDEV(heat_deaths_per_100k) AS s_heat
    FROM joined
)

SELECT
    j.nuts_id,
    j.country_code,
    j.country_name_en,
    j.name_latn,
    j.degurba_label,
    j.coast_label,
    j.centroid_lat,
    j.centroid_lon,
    j.excess_deaths_total,
    j.heat_deaths_per_100k,
    j.avg_pop_total,
    j.avg_share_65plus,
    j.n_sites_water,
    j.n_exceed_eu_dwd,
    j.exceedance_density_per_100k,
    j.max_pfas_ng_l_water,
    j.n_sites_manufacturer,
    j.n_sites_military,
    SAFE_DIVIDE(j.exceedance_density_per_100k - s.m_excd, NULLIF(s.s_excd, 0))
        AS burden_idx_pfas_only,
    (SAFE_DIVIDE(j.exceedance_density_per_100k - s.m_excd, NULLIF(s.s_excd, 0))
     + SAFE_DIVIDE(j.heat_deaths_per_100k - s.m_heat, NULLIF(s.s_heat, 0))) / 2
        AS burden_idx_heat_pfas
FROM joined j
CROSS JOIN stats s
ORDER BY burden_idx_pfas_only DESC NULLS LAST
