/* @bruin

name: staging.urban_trends
type: bq.sql
description: |
  Country-level urbanization time series pivoted from raw World Bank indicators.
  Provides the temporal dimension for the city-pulse dashboard: how each country's
  urban population share has evolved from 2000 to 2024.

  Transforms 6 World Bank development indicators from long format (indicator_code, value)
  to wide format with dedicated columns. Derives urbanization stage classification,
  5-year velocity metrics, decade grouping, World Bank regional assignments, and income
  group categorization from GDP per capita thresholds.

  Filters out 50+ World Bank aggregate entities (regional/income groups like 'AFE', 'WLD')
  using hardcoded exclusion list, retaining only sovereign countries with 3-character
  ISO codes. Final dataset spans 2020-2024 with 1,085 observations across 217 countries.

  Notable data characteristics: urbanization_velocity_5yr is entirely null (requires 5-year
  lag), largest_city_pct has 320 null values (~29%), gdp_per_capita missing for 61 observations
  affecting income_group classification.
connection: bruin-playground-arsalan
tags:
  - domain:urbanization
  - data_type:time_series
  - pipeline:city-pulse
  - data_source:world_bank
  - granularity:country-year
  - stage:staging
  - update_pattern:create_replace
  - geography:global
  - temporal_scope:2020-2024

materialization:
  type: table
  strategy: create+replace

depends:
  - raw.worldbank_urban

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: country_code
    type: VARCHAR
    description: |
      ISO 3166-1 alpha-3 country code. 217 distinct countries after filtering
      out World Bank regional aggregates. Always 3 characters, no nulls.
    primary_key: true
    checks:
      - name: not_null
  - name: year
    type: INTEGER
    description: |
      Calendar year (2020-2024). Data spans only 5 recent years despite pipeline
      targeting 2000-2024 range. No nulls.
    primary_key: true
    checks:
      - name: not_null
  - name: country_name
    type: VARCHAR
    description: |
      Full country name from World Bank. 217 distinct values matching country_code.
      Length ranges 4-30 characters. No nulls.
    checks:
      - name: not_null
  - name: urbanization_pct
    type: DOUBLE
    description: |
      Urban population as percentage of total population. Core urbanization metric
      from World Bank indicator SP.URB.TOTL.IN.ZS. Range 14.2%-100%, mean 62.3%.
      No nulls in current dataset.
    checks:
      - name: not_null
  - name: urban_growth_rate
    type: DOUBLE
    description: |
      Annual urban population growth rate (%). World Bank indicator SP.URB.GROW.
      Range -10.9% to +8.8%, mean 1.5%. One null value present.
  - name: largest_city_pct
    type: DOUBLE
    description: |
      Population in the largest city as percentage of urban population. World Bank
      indicator EN.URB.LCTY.UR.ZS. Range 3.0%-100%, but 320 null values (29.5%).
      Measures urban primacy - higher values indicate more centralized urbanization.
  - name: gdp_per_capita
    type: DOUBLE
    description: |
      GDP per capita in current US dollars. World Bank indicator NY.GDP.PCAP.CD.
      Range $219-$288,001, mean $20,463. 61 null values (5.6%) primarily affecting
      small island states and territories. Used to derive income_group classification.
  - name: total_population
    type: DOUBLE
    description: |
      Total country population. World Bank indicator SP.POP.TOTL.
      Range 9,646 to 1.45 billion, highly right-skewed. No nulls.
    checks:
      - name: not_null
  - name: pop_density
    type: DOUBLE
    description: |
      Population density in people per square kilometer. World Bank indicator
      EN.POP.DNST. Range 0.14 to 20,833, mean 450. 223 null values (20.6%)
      primarily for small island states without land area data.
  - name: urbanization_stage
    type: VARCHAR
    description: |
      Derived classification based on urbanization_pct thresholds:
      'Rural (<30%)', 'Transitioning (30-60%)', 'Urban (60-80%)', 'Hyper-urban (>80%)'.
      4 distinct values, no nulls. Average length 17.5 characters.
    checks:
      - name: not_null
  - name: urbanization_velocity_5yr
    type: DOUBLE
    description: |
      Change in urbanization percentage over previous 5 years using LAG(5) window.
      Currently all null (1,085 nulls) due to dataset spanning only 2020-2024,
      insufficient for 5-year lookback calculation.
  - name: decade
    type: INTEGER
    description: |
      Decade grouping derived as FLOOR(year/10)*10. Currently all values are 2020
      (stddev=0) since data spans only 2020-2024. No nulls.
    checks:
      - name: not_null
  - name: region
    type: VARCHAR
    description: |
      World Bank geographic region derived from hardcoded country_code mappings.
      8 distinct regions: East Asia & Pacific, Europe & Central Asia, Latin America
      & Caribbean, Middle East & North Africa, North America, South Asia,
      Sub-Saharan Africa, Other. No nulls.
    checks:
      - name: not_null
  - name: income_group
    type: VARCHAR
    description: |
      World Bank income classification derived from gdp_per_capita thresholds:
      'Low' (<$1,145), 'Lower-middle' ($1,145-$4,515), 'Upper-middle' ($4,516-$14,005),
      'High' (>$14,005). 4 distinct values, 61 nulls (5.6%) where gdp_per_capita is missing.
  - name: urban_population_est
    type: DOUBLE
    description: |
      Estimated urban population calculated as total_population * urbanization_pct / 100.
      Range 5,705 to 928 million, highly right-skewed. No nulls since both inputs
      are non-null in current dataset.
    checks:
      - name: not_null

@bruin */

WITH deduped AS (
    SELECT *
    FROM raw.worldbank_urban
    WHERE country_code IS NOT NULL
      AND value IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY country_code, indicator_code, year
        ORDER BY extracted_at DESC
    ) = 1
),

countries_only AS (
    SELECT *
    FROM deduped
    WHERE LENGTH(country_code) = 3
      AND country_code NOT IN (
        'AFE', 'AFW', 'ARB', 'CEB', 'CSS', 'EAP', 'EAR', 'EAS',
        'ECA', 'ECS', 'EMU', 'EUU', 'FCS', 'HIC', 'HPC', 'IBD',
        'IBT', 'IDA', 'IDB', 'IDX', 'INX', 'LAC', 'LCN', 'LDC',
        'LIC', 'LMC', 'LMY', 'LTE', 'MEA', 'MIC', 'MNA', 'NAC',
        'OED', 'OSS', 'PRE', 'PSS', 'PST', 'SAS', 'SSA', 'SSF',
        'SST', 'TEA', 'TEC', 'TLA', 'TMN', 'TSA', 'TSS', 'UMC',
        'WLD'
      )
),

pivoted AS (
    SELECT
        country_code,
        year,
        MAX(country_name) AS country_name,
        MAX(CASE WHEN indicator_code = 'SP.URB.TOTL.IN.ZS' THEN value END) AS urbanization_pct,
        MAX(CASE WHEN indicator_code = 'SP.URB.GROW' THEN value END) AS urban_growth_rate,
        MAX(CASE WHEN indicator_code = 'EN.URB.LCTY.UR.ZS' THEN value END) AS largest_city_pct,
        MAX(CASE WHEN indicator_code = 'NY.GDP.PCAP.CD' THEN value END) AS gdp_per_capita,
        MAX(CASE WHEN indicator_code = 'SP.POP.TOTL' THEN value END) AS total_population,
        MAX(CASE WHEN indicator_code = 'EN.POP.DNST' THEN value END) AS pop_density
    FROM countries_only
    GROUP BY country_code, year
),

with_derived AS (
    SELECT
        country_code,
        year,
        country_name,
        urbanization_pct,
        urban_growth_rate,
        largest_city_pct,
        gdp_per_capita,
        total_population,
        pop_density,

        CASE
            WHEN urbanization_pct IS NULL THEN NULL
            WHEN urbanization_pct < 30 THEN 'Rural (<30%)'
            WHEN urbanization_pct < 60 THEN 'Transitioning (30-60%)'
            WHEN urbanization_pct < 80 THEN 'Urban (60-80%)'
            ELSE 'Hyper-urban (>80%)'
        END AS urbanization_stage,

        ROUND(urbanization_pct - LAG(urbanization_pct, 5) OVER (
            PARTITION BY country_code ORDER BY year
        ), 2) AS urbanization_velocity_5yr,

        CAST(FLOOR(year / 10) * 10 AS INT64) AS decade,

        CASE
            WHEN country_code IN ('CHN', 'JPN', 'KOR', 'PRK', 'MNG', 'TWN', 'HKG', 'MAC',
                                  'IDN', 'THA', 'VNM', 'MYS', 'PHL', 'SGP', 'MMR', 'KHM',
                                  'LAO', 'BRN', 'TLS', 'AUS', 'NZL', 'PNG', 'FJI', 'SLB',
                                  'VUT', 'WSM', 'TON', 'FSM', 'KIR', 'MHL', 'PLW', 'TUV', 'NRU')
                THEN 'East Asia & Pacific'
            WHEN country_code IN ('GBR', 'FRA', 'DEU', 'ITA', 'ESP', 'PRT', 'NLD', 'BEL',
                                  'AUT', 'CHE', 'SWE', 'NOR', 'DNK', 'FIN', 'ISL', 'IRL',
                                  'LUX', 'GRC', 'POL', 'CZE', 'SVK', 'HUN', 'ROU', 'BGR',
                                  'HRV', 'SVN', 'SRB', 'BIH', 'MNE', 'MKD', 'ALB', 'KOS',
                                  'EST', 'LVA', 'LTU', 'UKR', 'BLR', 'MDA', 'RUS', 'GEO',
                                  'ARM', 'AZE', 'TUR', 'CYP', 'MLT', 'KAZ', 'UZB', 'TKM',
                                  'TJK', 'KGZ', 'AND', 'MCO', 'SMR', 'LIE', 'XKX')
                THEN 'Europe & Central Asia'
            WHEN country_code IN ('BRA', 'MEX', 'ARG', 'COL', 'PER', 'VEN', 'CHL', 'ECU',
                                  'BOL', 'PRY', 'URY', 'GUY', 'SUR', 'CUB', 'HTI', 'DOM',
                                  'JAM', 'TTO', 'PRI', 'CRI', 'PAN', 'GTM', 'HND', 'SLV',
                                  'NIC', 'BLZ', 'BHS', 'BRB', 'GRD', 'LCA', 'VCT', 'ATG',
                                  'DMA', 'KNA')
                THEN 'Latin America & Caribbean'
            WHEN country_code IN ('SAU', 'ARE', 'QAT', 'KWT', 'BHR', 'OMN', 'IRN', 'IRQ',
                                  'ISR', 'JOR', 'LBN', 'SYR', 'YEM', 'PSE', 'EGY', 'LBY',
                                  'TUN', 'DZA', 'MAR')
                THEN 'Middle East & North Africa'
            WHEN country_code IN ('IND', 'PAK', 'BGD', 'LKA', 'NPL', 'BTN', 'MDV', 'AFG')
                THEN 'South Asia'
            WHEN country_code IN ('NGA', 'ETH', 'COD', 'TZA', 'KEN', 'UGA', 'GHA', 'MOZ',
                                  'MDG', 'CMR', 'CIV', 'NER', 'BFA', 'MLI', 'MWI', 'ZMB',
                                  'SEN', 'TCD', 'SOM', 'ZWE', 'GIN', 'RWA', 'BDI', 'BEN',
                                  'TGO', 'SLE', 'LBR', 'CAF', 'ERI', 'MRT', 'NAM', 'BWA',
                                  'LSO', 'GMB', 'GAB', 'SWZ', 'COM', 'MUS', 'STP', 'CPV',
                                  'SYC', 'DJI', 'GNQ', 'GNB', 'COG', 'AGO', 'SSD', 'ZAF')
                THEN 'Sub-Saharan Africa'
            WHEN country_code IN ('USA', 'CAN')
                THEN 'North America'
            ELSE 'Other'
        END AS region,

        CASE
            WHEN gdp_per_capita IS NULL THEN NULL
            WHEN gdp_per_capita < 1145 THEN 'Low'
            WHEN gdp_per_capita < 4516 THEN 'Lower-middle'
            WHEN gdp_per_capita < 14006 THEN 'Upper-middle'
            ELSE 'High'
        END AS income_group,

        ROUND(COALESCE(total_population, 0) * COALESCE(urbanization_pct, 0) / 100) AS urban_population_est

    FROM pivoted
)

SELECT *
FROM with_derived
WHERE urbanization_pct IS NOT NULL OR gdp_per_capita IS NOT NULL
ORDER BY country_code, year
