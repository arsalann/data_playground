/* @bruin
name: staging.edc_h1_subregion_detail
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Per-eGRID-subregion view behind H1: how much data centre capacity sits in each
  grid region, and how dirty that region is. This is where the null headline
  result comes from - capacity is spread across both clean and dirty subregions
  rather than concentrating in dirty ones.

  Subregions with no data centres are retained with a zero count so the chart
  shows the full national distribution, not just the hosting regions.

depends:
  - staging.edc_facility_grid
  - raw.edc_egrid_subregion_rates

materialization:
  type: table
  strategy: create+replace

columns:
  - name: subregion_code
    type: VARCHAR
    description: eGRID subregion acronym
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: subregion_name
    type: VARCHAR
    description: Full eGRID subregion name
  - name: co2e_rate_g_per_kwh
    type: DOUBLE
    description: Subregion annual average CO2-equivalent output rate in grams per kWh
  - name: coal_generation_pct
    type: DOUBLE
    description: Percentage of subregion generation from coal
  - name: facility_count
    type: INTEGER
    description: Data centres assigned to this subregion, all statuses
  - name: operational_count
    type: INTEGER
    description: Data centres assigned to this subregion with operational status
  - name: total_capacity_mw
    type: DOUBLE
    description: Sum of reported capacity in MW across facilities in this subregion
  - name: share_of_facilities_pct
    type: DOUBLE
    description: This subregion's share of all assigned data centres
  - name: share_of_capacity_pct
    type: DOUBLE
    description: This subregion's share of all reported data centre capacity
  - name: share_of_us_generation_pct
    type: DOUBLE
    description: This subregion's share of US net generation, as a siting baseline

@bruin */

WITH by_subregion AS (
  SELECT
    subregion_code,
    COUNT(*) AS facility_count,
    COUNTIF(status = 'operational') AS operational_count,
    SUM(capacity_mw) AS total_capacity_mw
  FROM `bruin-playground-arsalan.staging.edc_facility_grid`
  WHERE subregion_code IS NOT NULL
  GROUP BY subregion_code
),

totals AS (
  SELECT
    SUM(facility_count) AS all_facilities,
    SUM(total_capacity_mw) AS all_capacity
  FROM by_subregion
),

generation_total AS (
  SELECT SUM(net_generation_mwh) AS us_generation
  FROM `bruin-playground-arsalan.raw.edc_egrid_subregion_rates`
  WHERE net_generation_mwh > 0
)

SELECT
  s.subregion_code,
  s.subregion_name,
  ROUND(s.co2e_rate_g_per_kwh, 1) AS co2e_rate_g_per_kwh,
  ROUND(s.coal_generation_pct, 1) AS coal_generation_pct,
  COALESCE(b.facility_count, 0) AS facility_count,
  COALESCE(b.operational_count, 0) AS operational_count,
  ROUND(COALESCE(b.total_capacity_mw, 0), 1) AS total_capacity_mw,
  ROUND(100 * COALESCE(b.facility_count, 0) / t.all_facilities, 2) AS share_of_facilities_pct,
  ROUND(100 * COALESCE(b.total_capacity_mw, 0) / t.all_capacity, 2) AS share_of_capacity_pct,
  ROUND(100 * s.net_generation_mwh / g.us_generation, 2) AS share_of_us_generation_pct
FROM `bruin-playground-arsalan.raw.edc_egrid_subregion_rates` s
LEFT JOIN by_subregion b ON b.subregion_code = s.subregion_code
CROSS JOIN totals t
CROSS JOIN generation_total g
WHERE s.net_generation_mwh > 0
ORDER BY co2e_rate_g_per_kwh DESC
