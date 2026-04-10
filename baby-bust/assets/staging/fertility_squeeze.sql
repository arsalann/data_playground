/* @bruin

name: staging.fertility_squeeze
type: bq.sql
description: |
  Comprehensive global fertility and development dataset spanning 1960-2024 for demographic transition analysis.

  Transforms raw World Bank indicators from long format into a denormalized country-year dataset suitable for
  fertility decline research. Covers 217 countries with 10 key development indicators including total fertility
  rate, economic measures (GDP PPP, GNI), social indicators (urbanization, female education/labor), and health
  metrics (life expectancy, infant mortality).

  Key analytical features:
  - Demographic transition stages based on fertility thresholds (pre-transition >5.0, early 3.0-5.0, late 2.1-3.0,
    below-replacement 1.5-2.1, crisis <1.5)
  - World Bank income classifications using current GNI thresholds (Low <$1,145, Lower-middle $1,145-4,515,
    Upper-middle $4,516-14,005, High >$14,005)
  - 5-year change metrics for fertility, GDP growth, and urbanization to capture trends
  - Regional groupings following World Bank classification system
  - Above-replacement fertility flag (>2.1 births per woman)

  Filters out 50+ World Bank aggregate entities (regional/income groups) to retain only sovereign countries.
  Deduplicates using latest extraction timestamp for data quality. Missing data varies by indicator and time
  period - GDP PPP has ~49% coverage, female tertiary enrollment ~33%, while core demographics have >99% coverage.
connection: bruin-playground-arsalan
tags:
  - domain:demographics
  - domain:economics
  - data_type:fact_table
  - data_type:time_series
  - sensitivity:public
  - pipeline_role:staging
  - update_pattern:snapshot
  - region:global
  - source:world_bank

materialization:
  type: table
  strategy: create+replace

depends:
  - raw.worldbank_indicators

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-3 country code identifier (e.g. USA, DEU, KOR). Primary key component. Excludes World Bank aggregate codes like 'WLD' or 'EAP'
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: year
    type: INTEGER
    description: Observation year (1960-2024). Primary key component. Annual time series data with some gaps for newer indicators
    primary_key: true
    nullable: false
    checks:
      - name: not_null
      - name: min
        value: 1960
      - name: max
        value: 2024
  - name: country_name
    type: VARCHAR
    description: Full country name as reported by World Bank (e.g. 'United States', 'Korea, Rep.'). May differ from common names due to diplomatic naming conventions
  - name: fertility_rate
    type: DOUBLE
    description: Total fertility rate - average number of children born per woman during reproductive lifetime. Key demographic indicator for population replacement analysis. Values range 0.582-8.864. Replacement level ~2.1 in developed countries
    checks:
      - name: non_negative
  - name: life_expectancy
    type: DOUBLE
    description: Life expectancy at birth in years. Strong correlate with development level and healthcare quality. Typically 50-86 years globally. Demographic dividend occurs when life expectancy rises before fertility falls
  - name: gdp_per_capita_ppp
    type: DOUBLE
    description: Gross domestic product per capita in purchasing power parity terms (current international dollars). Standardized for cost-of-living differences. Limited coverage ~49% due to data availability in early years
  - name: urbanization_pct
    type: DOUBLE
    description: Urban population as percentage of total population. Key driver of fertility decline through education access, delayed marriage, and changing economic incentives. Range 1.8-100%
    checks:
      - name: min
        value: 0
      - name: max
        value: 100
  - name: female_tertiary_enrollment
    type: DOUBLE
    description: Female gross enrollment ratio in tertiary education (percentage). Can exceed 100% due to over-age and grade repetition. Strong predictor of delayed childbearing. Limited coverage ~33% - primarily post-1970s data
  - name: female_labor_participation
    type: DOUBLE
    description: Female labor force participation rate - percentage of working-age women in labor force. Economic empowerment metric strongly associated with fertility decline. Range 4.7-90.5%
  - name: infant_mortality
    type: DOUBLE
    description: Infant mortality rate per 1,000 live births. Health development indicator inversely correlated with fertility as child survival improves. Range 1.2-484 per 1,000
  - name: health_expenditure_pct_gdp
    type: DOUBLE
    description: Current health expenditure as percentage of GDP including government and private spending. Healthcare investment metric affecting mortality rates and fertility decisions. Typically 1.2-27% of GDP
  - name: cpi
    type: DOUBLE
    description: Consumer price index with 2010 as base year (100). Inflation measure affecting real income and economic planning for families. High variance due to hyperinflation episodes in some countries
  - name: gni_per_capita
    type: DOUBLE
    description: Gross national income per capita in current US dollars. Used for World Bank income group classification thresholds. Differs from GDP by including net foreign income
  - name: income_group
    type: VARCHAR
    description: World Bank income classification based on GNI per capita thresholds. Low (<$1,145), Lower-middle ($1,145-4,515), Upper-middle ($4,516-14,005), High (>$14,005). Updated annually based on inflation
    checks:
      - name: accepted_values
        value:
          - Low
          - Lower-middle
          - Upper-middle
          - High
  - name: demographic_stage
    type: VARCHAR
    description: Demographic transition stage based on fertility rate thresholds. Pre-transition (>5.0), Early transition (3.0-5.0), Late transition (2.1-3.0), Below replacement (1.5-2.1), Demographic crisis (<1.5). Framework for understanding population dynamics
    checks:
      - name: accepted_values
        value:
          - Pre-transition
          - Early transition
          - Late transition
          - Below replacement
          - Demographic crisis
  - name: above_replacement
    type: BOOLEAN
    description: Boolean flag indicating whether fertility rate exceeds replacement level of 2.1 births per woman. Critical threshold for long-term population sustainability. True for ~72% of observations historically
  - name: region
    type: VARCHAR
    description: Geographic region following World Bank regional classification. Derived from country code mapping to 7 regions plus 'Other' for small island states. Used for regional fertility pattern analysis
  - name: fertility_change_5yr
    type: DOUBLE
    description: Absolute change in fertility rate over previous 5 years. Negative values indicate declining fertility. Useful for identifying accelerating demographic transitions. Range -3.227 to +3.477
  - name: gdp_growth_5yr_pct
    type: DOUBLE
    description: GDP per capita growth rate over 5-year period (percentage). Economic development trajectory affecting family planning decisions. Can be volatile due to economic crises and commodity cycles
  - name: urbanization_change_5yr
    type: DOUBLE
    description: Absolute change in urbanization percentage over 5 years. Positive values indicate rural-to-urban migration. Urbanization is major driver of fertility decline through changed living costs and social norms

@bruin */

WITH deduped AS (
    SELECT *
    FROM raw.worldbank_indicators
    WHERE country_code IS NOT NULL
      AND value IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY country_code, indicator_code, year
        ORDER BY extracted_at DESC
    ) = 1
),

-- Filter out World Bank aggregate entities (not real countries)
-- Aggregates have blank country_code or use special codes
countries_only AS (
    SELECT *
    FROM deduped
    WHERE LENGTH(country_code) = 3
      AND country_code NOT IN (
        -- World Bank aggregate codes
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
        MAX(CASE WHEN indicator_code = 'SP.DYN.TFRT.IN' THEN value END) AS fertility_rate,
        MAX(CASE WHEN indicator_code = 'SP.DYN.LE00.IN' THEN value END) AS life_expectancy,
        MAX(CASE WHEN indicator_code = 'NY.GDP.PCAP.PP.CD' THEN value END) AS gdp_per_capita_ppp,
        MAX(CASE WHEN indicator_code = 'SP.URB.TOTL.IN.ZS' THEN value END) AS urbanization_pct,
        MAX(CASE WHEN indicator_code = 'SE.TER.ENRR.FE' THEN value END) AS female_tertiary_enrollment,
        MAX(CASE WHEN indicator_code = 'SL.TLF.CACT.FE.ZS' THEN value END) AS female_labor_participation,
        MAX(CASE WHEN indicator_code = 'SP.DYN.IMRT.IN' THEN value END) AS infant_mortality,
        MAX(CASE WHEN indicator_code = 'SH.XPD.CHEX.GD.ZS' THEN value END) AS health_expenditure_pct_gdp,
        MAX(CASE WHEN indicator_code = 'FP.CPI.TOTL' THEN value END) AS cpi,
        MAX(CASE WHEN indicator_code = 'NY.GNP.PCAP.CD' THEN value END) AS gni_per_capita
    FROM countries_only
    GROUP BY country_code, year
),

with_derived AS (
    SELECT
        country_code,
        year,
        country_name,
        fertility_rate,
        life_expectancy,
        gdp_per_capita_ppp,
        urbanization_pct,
        female_tertiary_enrollment,
        female_labor_participation,
        infant_mortality,
        health_expenditure_pct_gdp,
        cpi,
        gni_per_capita,

        -- Income group classification (World Bank 2024 thresholds)
        CASE
            WHEN gni_per_capita IS NULL THEN NULL
            WHEN gni_per_capita < 1145 THEN 'Low'
            WHEN gni_per_capita < 4516 THEN 'Lower-middle'
            WHEN gni_per_capita < 14006 THEN 'Upper-middle'
            ELSE 'High'
        END AS income_group,

        -- Demographic stage based on fertility
        CASE
            WHEN fertility_rate IS NULL THEN NULL
            WHEN fertility_rate > 5.0 THEN 'Pre-transition'
            WHEN fertility_rate > 3.0 THEN 'Early transition'
            WHEN fertility_rate > 2.1 THEN 'Late transition'
            WHEN fertility_rate > 1.5 THEN 'Below replacement'
            ELSE 'Demographic crisis'
        END AS demographic_stage,

        -- Above replacement flag
        CASE WHEN fertility_rate > 2.1 THEN TRUE ELSE FALSE END AS above_replacement,

        -- Region mapping by country code
        CASE
            -- East Asia & Pacific
            WHEN country_code IN ('CHN', 'JPN', 'KOR', 'PRK', 'MNG', 'TWN', 'HKG', 'MAC',
                                  'IDN', 'THA', 'VNM', 'MYS', 'PHL', 'SGP', 'MMR', 'KHM',
                                  'LAO', 'BRN', 'TLS', 'AUS', 'NZL', 'PNG', 'FJI', 'SLB',
                                  'VUT', 'WSM', 'TON', 'FSM', 'KIR', 'MHL', 'PLW', 'TUV', 'NRU')
                THEN 'East Asia & Pacific'
            -- Europe & Central Asia
            WHEN country_code IN ('GBR', 'FRA', 'DEU', 'ITA', 'ESP', 'PRT', 'NLD', 'BEL',
                                  'AUT', 'CHE', 'SWE', 'NOR', 'DNK', 'FIN', 'ISL', 'IRL',
                                  'LUX', 'GRC', 'POL', 'CZE', 'SVK', 'HUN', 'ROU', 'BGR',
                                  'HRV', 'SVN', 'SRB', 'BIH', 'MNE', 'MKD', 'ALB', 'KOS',
                                  'EST', 'LVA', 'LTU', 'UKR', 'BLR', 'MDA', 'RUS', 'GEO',
                                  'ARM', 'AZE', 'TUR', 'CYP', 'MLT', 'KAZ', 'UZB', 'TKM',
                                  'TJK', 'KGZ', 'AND', 'MCO', 'SMR', 'LIE', 'XKX')
                THEN 'Europe & Central Asia'
            -- Latin America & Caribbean
            WHEN country_code IN ('BRA', 'MEX', 'ARG', 'COL', 'PER', 'VEN', 'CHL', 'ECU',
                                  'BOL', 'PRY', 'URY', 'GUY', 'SUR', 'CUB', 'HTI', 'DOM',
                                  'JAM', 'TTO', 'PRI', 'CRI', 'PAN', 'GTM', 'HND', 'SLV',
                                  'NIC', 'BLZ', 'BHS', 'BRB', 'GRD', 'LCA', 'VCT', 'ATG',
                                  'DMA', 'KNA')
                THEN 'Latin America & Caribbean'
            -- Middle East & North Africa
            WHEN country_code IN ('SAU', 'ARE', 'QAT', 'KWT', 'BHR', 'OMN', 'IRN', 'IRQ',
                                  'ISR', 'JOR', 'LBN', 'SYR', 'YEM', 'PSE', 'EGY', 'LBY',
                                  'TUN', 'DZA', 'MAR')
                THEN 'Middle East & North Africa'
            -- South Asia
            WHEN country_code IN ('IND', 'PAK', 'BGD', 'LKA', 'NPL', 'BTN', 'MDV', 'AFG')
                THEN 'South Asia'
            -- Sub-Saharan Africa
            WHEN country_code IN ('NGA', 'ETH', 'COD', 'TZA', 'KEN', 'UGA', 'GHA', 'MOZ',
                                  'MDG', 'CMR', 'CIV', 'NER', 'BFA', 'MLI', 'MWI', 'ZMB',
                                  'SEN', 'TCD', 'SOM', 'ZWE', 'GIN', 'RWA', 'BDI', 'BEN',
                                  'TGO', 'SLE', 'LBR', 'CAF', 'ERI', 'MRT', 'NAM', 'BWA',
                                  'LSO', 'GMB', 'GAB', 'SWZ', 'COM', 'MUS', 'STP', 'CPV',
                                  'SYC', 'DJI', 'GNQ', 'GNB', 'COG', 'AGO', 'SSD', 'ZAF')
                THEN 'Sub-Saharan Africa'
            -- North America
            WHEN country_code IN ('USA', 'CAN')
                THEN 'North America'
            ELSE 'Other'
        END AS region,

        -- 5-year fertility change
        ROUND(fertility_rate - LAG(fertility_rate, 5) OVER (
            PARTITION BY country_code ORDER BY year
        ), 3) AS fertility_change_5yr,

        -- 5-year GDP growth %
        ROUND(
            SAFE_DIVIDE(
                gdp_per_capita_ppp - LAG(gdp_per_capita_ppp, 5) OVER (PARTITION BY country_code ORDER BY year),
                LAG(gdp_per_capita_ppp, 5) OVER (PARTITION BY country_code ORDER BY year)
            ) * 100,
            2
        ) AS gdp_growth_5yr_pct,

        -- 5-year urbanization change
        ROUND(urbanization_pct - LAG(urbanization_pct, 5) OVER (
            PARTITION BY country_code ORDER BY year
        ), 2) AS urbanization_change_5yr

    FROM pivoted
)

SELECT *
FROM with_derived
WHERE fertility_rate IS NOT NULL
   OR gdp_per_capita_ppp IS NOT NULL
   OR life_expectancy IS NOT NULL
ORDER BY country_code, year
