/* @bruin
name: numbeo_report.city_squeeze_top
type: bq.sql
connection: bruin-playground-arsalan
description: Top cities where combined cost plus rent most exceeds local purchasing power.
depends:
  - numbeo_staging.city_cost_metrics

materialization:
  type: table
  strategy: create+replace

columns:
  - name: city
    type: VARCHAR
    description: City name.
    primary_key: true
  - name: country
    type: VARCHAR
    description: Country or territory.
  - name: affordability_gap_index
    type: DOUBLE
    description: Combined cost plus rent minus local purchasing power index points.
  - name: cost_of_living_plus_rent_index
    type: DOUBLE
    description: Combined cost and rent index relative to New York City = 100.
  - name: local_purchasing_power_index
    type: DOUBLE
    description: Local purchasing power index relative to New York City = 100.
  - name: rent_to_purchasing_power_ratio
    type: DOUBLE
    description: Rent index divided by local purchasing power index.

@bruin */

WITH latest AS (
    SELECT MAX(snapshot_date) AS snapshot_date
    FROM numbeo_staging.city_cost_metrics
)

SELECT
    city,
    country,
    affordability_gap_index,
    cost_of_living_plus_rent_index,
    local_purchasing_power_index,
    rent_to_purchasing_power_ratio
FROM numbeo_staging.city_cost_metrics
WHERE affordability_gap_index > 0
  AND snapshot_date = (SELECT snapshot_date FROM latest)
ORDER BY affordability_gap_index DESC
LIMIT 12
