/* @bruin
name: staging.aep_electricity_by_source
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Transforms OWID energy data into electricity-by-source analysis table.
  One row per country-year with generation by source (TWh), total generation,
  total demand, emissions, renewable/fossil shares, and YoY growth rates.

  Sources from OWID which includes Ember data.

depends:
  - raw.aep_owid_energy

materialization:
  type: table
  strategy: create+replace

columns:
  - name: country_or_area
    type: VARCHAR
    description: Country or region name
    primary_key: true
  - name: year
    type: INTEGER
    description: Year of the observation
    primary_key: true
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-3 country code
  - name: coal_twh
    type: DOUBLE
    description: Electricity generation from coal (TWh)
  - name: gas_twh
    type: DOUBLE
    description: Electricity generation from gas (TWh)
  - name: oil_twh
    type: DOUBLE
    description: Electricity generation from oil (TWh)
  - name: nuclear_twh
    type: DOUBLE
    description: Electricity generation from nuclear (TWh)
  - name: hydro_twh
    type: DOUBLE
    description: Electricity generation from hydro (TWh)
  - name: wind_twh
    type: DOUBLE
    description: Electricity generation from wind (TWh)
  - name: solar_twh
    type: DOUBLE
    description: Electricity generation from solar (TWh)
  - name: total_generation_twh
    type: DOUBLE
    description: Total electricity generation (TWh)
  - name: total_demand_twh
    type: DOUBLE
    description: Total electricity demand (TWh)
  - name: co2_emissions_mt
    type: DOUBLE
    description: CO2 emissions from electricity generation (MtCO2), derived from carbon intensity
  - name: emissions_intensity_gco2kwh
    type: DOUBLE
    description: Carbon intensity of electricity (gCO2/kWh)
  - name: renewables_twh
    type: DOUBLE
    description: Total renewables generation = wind + solar + hydro + other_renewable (TWh)
  - name: fossil_twh
    type: DOUBLE
    description: Total fossil generation = coal + gas + oil (TWh)
  - name: renewable_share_pct
    type: DOUBLE
    description: Renewables as share of total generation (%)
  - name: coal_share_pct
    type: DOUBLE
    description: Coal as share of total generation (%)
  - name: fossil_share_pct
    type: DOUBLE
    description: Fossil fuels as share of total generation (%)
  - name: generation_yoy_pct
    type: DOUBLE
    description: Year-over-year change in total generation (%)
  - name: renewables_yoy_pct
    type: DOUBLE
    description: Year-over-year change in renewables generation (%)

@bruin */

WITH deduped AS (
    SELECT *
    FROM raw.aep_owid_energy
    WHERE country IS NOT NULL
      AND year IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY country, year
        ORDER BY extracted_at DESC
    ) = 1
),

base AS (
    SELECT
        country AS country_or_area,
        year,
        iso_code AS country_code,
        coal_electricity AS coal_twh,
        gas_electricity AS gas_twh,
        oil_electricity AS oil_twh,
        nuclear_electricity AS nuclear_twh,
        hydro_electricity AS hydro_twh,
        wind_electricity AS wind_twh,
        solar_electricity AS solar_twh,
        electricity_generation AS total_generation_twh,
        electricity_demand AS total_demand_twh,
        carbon_intensity_elec AS emissions_intensity_gco2kwh,
        other_renewable_electricity,
        renewables_share_elec,
        fossil_share_elec
    FROM deduped
),

with_derived AS (
    SELECT
        country_or_area,
        year,
        country_code,
        coal_twh,
        gas_twh,
        oil_twh,
        nuclear_twh,
        hydro_twh,
        wind_twh,
        solar_twh,
        total_generation_twh,
        total_demand_twh,
        emissions_intensity_gco2kwh,
        -- Estimate CO2 from generation and intensity: TWh * 1e6 MWh/TWh * gCO2/kWh * 1e-6 MtCO2/gCO2 = TWh * intensity / 1000
        ROUND(total_generation_twh * emissions_intensity_gco2kwh / 1000, 2) AS co2_emissions_mt,
        ROUND(COALESCE(wind_twh, 0) + COALESCE(solar_twh, 0) + COALESCE(hydro_twh, 0)
              + COALESCE(other_renewable_electricity, 0), 2) AS renewables_twh,
        ROUND(COALESCE(coal_twh, 0) + COALESCE(gas_twh, 0) + COALESCE(oil_twh, 0), 2) AS fossil_twh,
        COALESCE(renewables_share_elec, ROUND(SAFE_DIVIDE(
            COALESCE(wind_twh, 0) + COALESCE(solar_twh, 0) + COALESCE(hydro_twh, 0)
            + COALESCE(other_renewable_electricity, 0),
            total_generation_twh
        ) * 100, 2)) AS renewable_share_pct,
        ROUND(SAFE_DIVIDE(coal_twh, total_generation_twh) * 100, 2) AS coal_share_pct,
        COALESCE(fossil_share_elec, ROUND(SAFE_DIVIDE(
            COALESCE(coal_twh, 0) + COALESCE(gas_twh, 0) + COALESCE(oil_twh, 0),
            total_generation_twh
        ) * 100, 2)) AS fossil_share_pct,
        ROUND(SAFE_DIVIDE(
            total_generation_twh - LAG(total_generation_twh) OVER (PARTITION BY country_or_area ORDER BY year),
            LAG(total_generation_twh) OVER (PARTITION BY country_or_area ORDER BY year)
        ) * 100, 2) AS generation_yoy_pct,
        ROUND(SAFE_DIVIDE(
            (COALESCE(wind_twh, 0) + COALESCE(solar_twh, 0) + COALESCE(hydro_twh, 0)
             + COALESCE(other_renewable_electricity, 0))
            - LAG(COALESCE(wind_twh, 0) + COALESCE(solar_twh, 0) + COALESCE(hydro_twh, 0)
                  + COALESCE(other_renewable_electricity, 0))
              OVER (PARTITION BY country_or_area ORDER BY year),
            LAG(COALESCE(wind_twh, 0) + COALESCE(solar_twh, 0) + COALESCE(hydro_twh, 0)
                + COALESCE(other_renewable_electricity, 0))
              OVER (PARTITION BY country_or_area ORDER BY year)
        ) * 100, 2) AS renewables_yoy_pct
    FROM base
    WHERE total_generation_twh IS NOT NULL
)

SELECT *
FROM with_derived
ORDER BY country_or_area, year
