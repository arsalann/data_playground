/* @bruin
name: staging.aep_demand_growth
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Combines data center projections, EV battery demand, and total
  electricity demand into a unified timeline for comparing new demand
  sources. One row per year-demand_source with TWh values.

depends:
  - raw.aep_datacenter_demand
  - raw.aep_ev_demand
  - staging.aep_electricity_by_source

materialization:
  type: table
  strategy: create+replace

columns:
  - name: year
    type: INTEGER
    description: Year
    primary_key: true
  - name: demand_source
    type: VARCHAR
    description: "Source of demand: Data Centers, AI Servers, Electric Vehicles, Total Electricity, Renewables"
    primary_key: true
  - name: value_twh
    type: DOUBLE
    description: Electricity demand or generation (TWh)
  - name: is_projection
    type: BOOLEAN
    description: True if this is a forward projection rather than historical data
  - name: source
    type: VARCHAR
    description: Data source for traceability

@bruin */

WITH dc_total AS (
    SELECT
        year,
        'Data Centers' AS demand_source,
        value AS value_twh,
        is_projection,
        source
    FROM raw.aep_datacenter_demand
    WHERE category = 'global_dc'
      AND metric_name = 'electricity_demand_twh'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY year ORDER BY extracted_at DESC) = 1
),

ai_servers AS (
    SELECT
        year,
        'AI Servers' AS demand_source,
        value AS value_twh,
        is_projection,
        source
    FROM raw.aep_datacenter_demand
    WHERE category = 'ai_servers'
      AND metric_name = 'electricity_demand_twh'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY year ORDER BY extracted_at DESC) = 1
),

ev_demand AS (
    SELECT
        year,
        'Electric Vehicles' AS demand_source,
        ROUND(value / 1000, 2) AS value_twh,
        CASE WHEN LOWER(category) = 'projection' THEN TRUE ELSE FALSE END AS is_projection,
        'IEA Global EV Data Explorer 2025' AS source
    FROM raw.aep_ev_demand
    WHERE LOWER(parameter) = 'battery demand'
      AND LOWER(region) = 'world'
      AND LOWER(powertrain) = 'ev'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY year ORDER BY extracted_at DESC) = 1
),

total_electricity AS (
    SELECT
        year,
        'Total Electricity' AS demand_source,
        total_generation_twh AS value_twh,
        FALSE AS is_projection,
        'OWID Energy Data (Ember)' AS source
    FROM staging.aep_electricity_by_source
    WHERE LOWER(country_or_area) = 'world'
      AND total_generation_twh IS NOT NULL
),

renewables_gen AS (
    SELECT
        year,
        'Renewables' AS demand_source,
        renewables_twh AS value_twh,
        FALSE AS is_projection,
        'OWID Energy Data (Ember)' AS source
    FROM staging.aep_electricity_by_source
    WHERE LOWER(country_or_area) = 'world'
      AND renewables_twh IS NOT NULL
)

SELECT * FROM dc_total
UNION ALL
SELECT * FROM ai_servers
UNION ALL
SELECT * FROM ev_demand
UNION ALL
SELECT * FROM total_electricity
UNION ALL
SELECT * FROM renewables_gen
ORDER BY year, demand_source
