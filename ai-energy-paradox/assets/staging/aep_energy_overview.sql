/* @bruin
name: staging.aep_energy_overview
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Cleans OWID energy data and computes per-capita and energy intensity
  metrics. Provides GDP and population normalization for cross-country
  comparisons. One row per country-year.

depends:
  - raw.aep_owid_energy

materialization:
  type: table
  strategy: create+replace

columns:
  - name: country
    type: VARCHAR
    description: Country or region name
    primary_key: true
  - name: year
    type: INTEGER
    description: Year of the observation
    primary_key: true
  - name: iso_code
    type: VARCHAR
    description: ISO 3166-1 alpha-3 country code
  - name: population
    type: DOUBLE
    description: Total population
  - name: gdp
    type: DOUBLE
    description: GDP in international dollars (PPP, 2017 prices)
  - name: gdp_per_capita
    type: DOUBLE
    description: GDP per capita (international dollars)
  - name: electricity_generation
    type: DOUBLE
    description: Total electricity generation (TWh)
  - name: electricity_demand
    type: DOUBLE
    description: Total electricity demand (TWh)
  - name: electricity_per_capita_kwh
    type: DOUBLE
    description: Electricity generation per capita (kWh)
  - name: energy_per_capita_kwh
    type: DOUBLE
    description: Primary energy consumption per capita (kWh)
  - name: energy_intensity
    type: DOUBLE
    description: Primary energy per unit GDP (kWh per int dollar)
  - name: coal_electricity
    type: DOUBLE
    description: Electricity from coal (TWh)
  - name: gas_electricity
    type: DOUBLE
    description: Electricity from gas (TWh)
  - name: oil_electricity
    type: DOUBLE
    description: Electricity from oil (TWh)
  - name: nuclear_electricity
    type: DOUBLE
    description: Electricity from nuclear (TWh)
  - name: hydro_electricity
    type: DOUBLE
    description: Electricity from hydro (TWh)
  - name: wind_electricity
    type: DOUBLE
    description: Electricity from wind (TWh)
  - name: solar_electricity
    type: DOUBLE
    description: Electricity from solar (TWh)
  - name: renewables_share_elec
    type: DOUBLE
    description: Share of electricity from renewables (%)
  - name: fossil_share_elec
    type: DOUBLE
    description: Share of electricity from fossil fuels (%)
  - name: nuclear_share_elec
    type: DOUBLE
    description: Share of electricity from nuclear (%)
  - name: carbon_intensity_elec
    type: DOUBLE
    description: Carbon intensity of electricity (gCO2/kWh)
  - name: primary_energy_consumption
    type: DOUBLE
    description: Primary energy consumption (TWh)

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
)

SELECT
    country,
    year,
    iso_code,
    population,
    gdp,
    ROUND(SAFE_DIVIDE(gdp, population), 2) AS gdp_per_capita,
    electricity_generation,
    electricity_demand,
    ROUND(SAFE_DIVIDE(electricity_generation * 1e9, population), 0) AS electricity_per_capita_kwh,
    ROUND(SAFE_DIVIDE(primary_energy_consumption * 1e9, population), 0) AS energy_per_capita_kwh,
    ROUND(SAFE_DIVIDE(primary_energy_consumption * 1e9, gdp), 4) AS energy_intensity,
    coal_electricity,
    gas_electricity,
    oil_electricity,
    nuclear_electricity,
    hydro_electricity,
    wind_electricity,
    solar_electricity,
    renewables_share_elec,
    fossil_share_elec,
    nuclear_share_elec,
    carbon_intensity_elec,
    primary_energy_consumption
FROM deduped
ORDER BY country, year
