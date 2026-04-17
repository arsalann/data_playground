/* @bruin
name: staging.aep_emissions_impact
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Estimates data center emissions impact by country. Distributes global
  DC electricity demand proportionally by each country's share of global
  generation, then computes emissions at the country's grid carbon intensity.
  Filters to actual countries only (excludes aggregates like G20, OECD, etc.).

depends:
  - staging.aep_electricity_by_source
  - raw.aep_datacenter_demand

materialization:
  type: table
  strategy: create+replace

columns:
  - name: country_or_area
    type: VARCHAR
    description: Country or region name
    primary_key: true
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-3 country code
  - name: year
    type: INTEGER
    description: Year
    primary_key: true
  - name: total_generation_twh
    type: DOUBLE
    description: Country total electricity generation (TWh)
  - name: co2_emissions_mt
    type: DOUBLE
    description: Country total electricity CO2 emissions (MtCO2)
  - name: emissions_intensity_gco2kwh
    type: DOUBLE
    description: Country grid carbon intensity (gCO2/kWh)
  - name: coal_share_pct
    type: DOUBLE
    description: Coal share of generation (%)
  - name: renewable_share_pct
    type: DOUBLE
    description: Renewables share of generation (%)
  - name: global_dc_demand_twh
    type: DOUBLE
    description: Global data center electricity demand for that year (TWh)
  - name: country_generation_share_pct
    type: DOUBLE
    description: Country share of global electricity generation (%)
  - name: estimated_dc_demand_twh
    type: DOUBLE
    description: Estimated DC demand in this country (proportional to generation share, TWh)
  - name: estimated_dc_emissions_mt
    type: DOUBLE
    description: Estimated CO2 from DC demand at this country's grid intensity (MtCO2)
  - name: dc_emissions_per_twh_mt
    type: DOUBLE
    description: CO2 emitted per TWh of DC demand in this country (MtCO2/TWh)

@bruin */

WITH global_generation AS (
    SELECT
        year,
        total_generation_twh AS global_total_twh
    FROM staging.aep_electricity_by_source
    WHERE LOWER(country_or_area) = 'world'
),

country_data AS (
    SELECT
        e.country_or_area,
        e.country_code,
        e.year,
        e.total_generation_twh,
        e.co2_emissions_mt,
        e.emissions_intensity_gco2kwh,
        e.coal_share_pct,
        e.renewable_share_pct
    FROM staging.aep_electricity_by_source e
    WHERE LOWER(e.country_or_area) != 'world'
      AND e.total_generation_twh IS NOT NULL
      AND e.total_generation_twh > 0
      AND e.emissions_intensity_gco2kwh IS NOT NULL
      AND e.country_code IS NOT NULL
      AND LENGTH(e.country_code) = 3
),

dc_demand AS (
    SELECT
        year,
        value AS global_dc_demand_twh
    FROM raw.aep_datacenter_demand
    WHERE category = 'global_dc'
      AND metric_name = 'electricity_demand_twh'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY year ORDER BY extracted_at DESC) = 1
)

SELECT
    c.country_or_area,
    c.country_code,
    c.year,
    c.total_generation_twh,
    c.co2_emissions_mt,
    c.emissions_intensity_gco2kwh,
    c.coal_share_pct,
    c.renewable_share_pct,
    d.global_dc_demand_twh,
    ROUND(SAFE_DIVIDE(c.total_generation_twh, g.global_total_twh) * 100, 4) AS country_generation_share_pct,
    ROUND(d.global_dc_demand_twh * SAFE_DIVIDE(c.total_generation_twh, g.global_total_twh), 4) AS estimated_dc_demand_twh,
    ROUND(
        d.global_dc_demand_twh
        * SAFE_DIVIDE(c.total_generation_twh, g.global_total_twh)
        * c.emissions_intensity_gco2kwh / 1000,
        4
    ) AS estimated_dc_emissions_mt,
    ROUND(c.emissions_intensity_gco2kwh / 1000, 4) AS dc_emissions_per_twh_mt
FROM country_data c
INNER JOIN global_generation g ON c.year = g.year
INNER JOIN dc_demand d ON c.year = d.year
ORDER BY c.year, c.total_generation_twh DESC
