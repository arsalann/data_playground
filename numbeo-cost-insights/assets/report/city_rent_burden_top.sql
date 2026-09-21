/* @bruin
name: numbeo_report.city_rent_burden_top
type: bq.sql
connection: bruin-playground-arsalan
description: Cities with the highest indexed rent burden relative to local purchasing power.
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
  - name: rent_to_purchasing_power_ratio
    type: DOUBLE
    description: Rent index divided by local purchasing power index.
  - name: rent_index
    type: DOUBLE
    description: Rent index relative to New York City = 100.
  - name: local_purchasing_power_index
    type: DOUBLE
    description: Local purchasing power index relative to New York City = 100.

@bruin */

WITH latest AS (
    SELECT MAX(snapshot_date) AS snapshot_date
    FROM numbeo_staging.city_cost_metrics
)

SELECT
    city,
    country,
    rent_to_purchasing_power_ratio,
    rent_index,
    local_purchasing_power_index
FROM numbeo_staging.city_cost_metrics
WHERE snapshot_date = (SELECT snapshot_date FROM latest)
  AND rent_to_purchasing_power_ratio IS NOT NULL
ORDER BY rent_to_purchasing_power_ratio DESC
LIMIT 12
