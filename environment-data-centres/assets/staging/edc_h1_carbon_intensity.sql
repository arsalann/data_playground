/* @bruin
name: staging.edc_h1_carbon_intensity
type: bq.sql
connection: bruin-playground-arsalan
description: |
  H1: "US data centres are sited on dirtier-than-average grids."

  Result: REJECTED. Across every weighting and subset tested, the emissions
  intensity of the grid regions hosting US data centres lands between 6.7%
  below and 1.0% above the national generation-weighted average. There is no
  dirty-siting signal in this data.

  The national benchmark is computed from the same eGRID subregion table,
  weighted by subregion net generation, so benchmark and measurement share a
  vintage and a method.

  Two weightings are reported so the reader can see the result is not an
  artefact of one choice: unweighted rows treat every site equally, capacity
  weighted rows reflect only the facilities that publish a capacity figure.

depends:
  - staging.edc_facility_grid
  - raw.edc_egrid_subregion_rates

materialization:
  type: table
  strategy: create+replace

columns:
  - name: cut_label
    type: VARCHAR
    description: Which subset of facilities and which weighting this row measures
    primary_key: true
  - name: sort_order
    type: INTEGER
    description: Display order, ascending
  - name: facility_count
    type: INTEGER
    description: Number of facilities in this cut
  - name: dc_co2e_g_per_kwh
    type: DOUBLE
    description: Emissions intensity of the grid regions hosting these facilities, in gCO2e per kWh
  - name: us_avg_co2e_g_per_kwh
    type: DOUBLE
    description: US generation-weighted average across all eGRID subregions, same vintage
  - name: pct_vs_us_average
    type: DOUBLE
    description: Percentage difference from the national average; positive means dirtier
  - name: weighting
    type: VARCHAR
    description: "Either unweighted or capacity_weighted"

@bruin */

WITH us_benchmark AS (
  SELECT SUM(co2e_rate_g_per_kwh * net_generation_mwh) / SUM(net_generation_mwh) AS us_avg
  FROM `bruin-playground-arsalan.raw.edc_egrid_subregion_rates`
  WHERE net_generation_mwh > 0 AND co2e_rate_g_per_kwh IS NOT NULL
),

grid AS (
  SELECT * FROM `bruin-playground-arsalan.staging.edc_facility_grid`
  WHERE co2e_rate_g_per_kwh IS NOT NULL
),

cuts AS (
  SELECT 'All facilities' AS cut_label, 1 AS sort_order, 'unweighted' AS weighting,
         COUNT(*) AS facility_count, AVG(co2e_rate_g_per_kwh) AS dc_rate
  FROM grid

  UNION ALL
  SELECT 'All, capacity-weighted', 2, 'capacity_weighted',
         COUNT(*), SUM(co2e_rate_g_per_kwh * capacity_mw) / SUM(capacity_mw)
  FROM grid WHERE capacity_mw > 0

  UNION ALL
  SELECT 'Operational only', 3, 'unweighted', COUNT(*), AVG(co2e_rate_g_per_kwh)
  FROM grid WHERE status = 'operational'

  UNION ALL
  SELECT 'Operational, capacity-weighted', 4, 'capacity_weighted',
         COUNT(*), SUM(co2e_rate_g_per_kwh * capacity_mw) / SUM(capacity_mw)
  FROM grid WHERE status = 'operational' AND capacity_mw > 0

  UNION ALL
  SELECT 'Pre-operational', 5, 'unweighted',
         COUNT(*), AVG(co2e_rate_g_per_kwh)
  FROM grid WHERE status IN ('proposed', 'permitted', 'under_construction')

  UNION ALL
  -- Guards against subregion misassignment near boundaries.
  SELECT 'Within 25 km of plant', 6, 'unweighted',
         COUNT(*), AVG(co2e_rate_g_per_kwh)
  FROM grid WHERE nearest_plant_km < 25
)

SELECT
  c.cut_label,
  c.sort_order,
  c.facility_count,
  ROUND(c.dc_rate, 1) AS dc_co2e_g_per_kwh,
  ROUND(b.us_avg, 1) AS us_avg_co2e_g_per_kwh,
  ROUND(100 * (c.dc_rate / b.us_avg - 1), 1) AS pct_vs_us_average,
  c.weighting
FROM cuts c
CROSS JOIN us_benchmark b
ORDER BY c.sort_order
