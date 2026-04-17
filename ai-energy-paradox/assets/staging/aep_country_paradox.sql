/* @bruin
name: staging.aep_country_paradox
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Per-country analysis combining renewable energy share, data center intensity,
  emissions intensity, and economic indicators. Classifies countries into
  paradox categories based on their green grid vs data center consumption.

depends:
  - staging.aep_electricity_by_source
  - staging.aep_energy_overview
  - raw.aep_datacenter_demand

materialization:
  type: table
  strategy: create+replace

columns:
  - name: country
    type: VARCHAR
    description: Country name
    primary_key: true
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-3 country code
  - name: year
    type: INTEGER
    description: Most recent year with data
  - name: renewable_share_pct
    type: DOUBLE
    description: Renewables as share of total generation (%)
  - name: coal_share_pct
    type: DOUBLE
    description: Coal as share of total generation (%)
  - name: fossil_share_pct
    type: DOUBLE
    description: Fossil fuels as share of total generation (%)
  - name: emissions_intensity_gco2kwh
    type: DOUBLE
    description: Carbon intensity of the electricity grid (gCO2/kWh)
  - name: total_generation_twh
    type: DOUBLE
    description: Total electricity generation (TWh)
  - name: renewables_twh
    type: DOUBLE
    description: Total renewables generation (TWh)
  - name: gdp_per_capita
    type: DOUBLE
    description: GDP per capita (international dollars)
  - name: population
    type: DOUBLE
    description: Total population
  - name: electricity_per_capita_kwh
    type: DOUBLE
    description: Electricity generation per capita (kWh)
  - name: dc_share_of_electricity_pct
    type: DOUBLE
    description: Data center share of national electricity (%, where available)
  - name: paradox_category
    type: VARCHAR
    description: "Classification: Green Grid High DC, Dirty Grid High DC, Green Grid Low DC, Other"

@bruin */

WITH country_elec AS (
    SELECT
        e.country_or_area AS country,
        e.country_code,
        e.year,
        e.renewable_share_pct,
        e.coal_share_pct,
        e.fossil_share_pct,
        e.emissions_intensity_gco2kwh,
        e.total_generation_twh,
        e.renewables_twh
    FROM staging.aep_electricity_by_source e
    WHERE e.total_generation_twh IS NOT NULL
      AND e.total_generation_twh > 0
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY e.country_or_area
        ORDER BY e.year DESC
    ) = 1
),

country_econ AS (
    SELECT
        o.country AS econ_country,
        o.iso_code,
        o.gdp_per_capita,
        o.population,
        o.electricity_per_capita_kwh
    FROM staging.aep_energy_overview o
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY o.country
        ORDER BY o.year DESC
    ) = 1
),

dc_shares AS (
    SELECT
        CASE
            WHEN region = 'US' THEN 'United States'
            WHEN region = 'UK' THEN 'United Kingdom'
            ELSE region
        END AS region_normalized,
        value AS dc_share_of_electricity_pct
    FROM raw.aep_datacenter_demand
    WHERE category = 'national_share'
      AND metric_name = 'dc_share_of_electricity_pct'
      AND is_projection = FALSE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY year DESC, extracted_at DESC) = 1
)

SELECT
    e.country,
    COALESCE(e.country_code, o.iso_code) AS country_code,
    e.year,
    e.renewable_share_pct,
    e.coal_share_pct,
    e.fossil_share_pct,
    e.emissions_intensity_gco2kwh,
    e.total_generation_twh,
    e.renewables_twh,
    o.gdp_per_capita,
    o.population,
    o.electricity_per_capita_kwh,
    d.dc_share_of_electricity_pct,
    CASE
        WHEN e.renewable_share_pct >= 35 AND d.dc_share_of_electricity_pct >= 4
            THEN 'Green Grid, High DC'
        WHEN e.renewable_share_pct < 25 AND d.dc_share_of_electricity_pct >= 2
            THEN 'Fossil Grid, High DC'
        WHEN e.renewable_share_pct >= 35 AND d.dc_share_of_electricity_pct IS NOT NULL AND d.dc_share_of_electricity_pct < 4
            THEN 'Green Grid, Some DC'
        WHEN d.dc_share_of_electricity_pct IS NOT NULL
            THEN 'Mixed Grid, Some DC'
        ELSE 'No DC Data'
    END AS paradox_category
FROM country_elec e
LEFT JOIN country_econ o
    ON LOWER(TRIM(e.country)) = LOWER(TRIM(o.econ_country))
LEFT JOIN dc_shares d
    ON LOWER(TRIM(e.country)) = LOWER(TRIM(d.region_normalized))
ORDER BY e.renewable_share_pct DESC
