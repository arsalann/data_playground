/* @bruin
name: staging.regional_affordability
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Analysis-ready OECD TL2 regional affordability panel for Europe and North
  America. Joins primary income, disposable income, and housing-cost burden at
  the same region-year grain, excluding any region-year where one of the three
  required metrics is missing.

depends:
  - raw.oecd_tl2_income
  - raw.oecd_tl2_housing

materialization:
  type: table
  strategy: create+replace

columns:
  - name: region_code
    type: VARCHAR
    description: OECD TL2 reference area code.
    primary_key: true
    nullable: false
  - name: year
    type: INTEGER
    description: Calendar year where all required affordability metrics are present.
    primary_key: true
    nullable: false
  - name: region_name
    type: VARCHAR
    description: Human-readable OECD TL2 region name.
  - name: country_prefix
    type: VARCHAR
    description: OECD country prefix used in regional codes.
  - name: country_name
    type: VARCHAR
    description: Country name normalized from the OECD regional code prefix.
  - name: macro_region
    type: VARCHAR
    description: Europe or North America, used for dashboard filtering and analysis.
  - name: primary_income_ppp_pc
    type: DOUBLE
    description: Net primary income per person in current-price USD PPP.
  - name: disposable_income_ppp_pc
    type: DOUBLE
    description: Net disposable income per person in current-price USD PPP.
  - name: tax_transfer_gap_ppp_pc
    type: DOUBLE
    description: Primary income minus disposable income, in current-price USD PPP per person.
  - name: tax_transfer_gap_pct
    type: DOUBLE
    description: Primary-to-disposable income gap as percent of primary income.
  - name: housing_cost_pct_disposable_income
    type: DOUBLE
    description: Housing cost burden as percent of net disposable household income.
  - name: disposable_after_housing_ppp_pc
    type: DOUBLE
    description: Estimated disposable income after applying regional housing-cost burden, in USD PPP per person.
  - name: affordability_score
    type: DOUBLE
    description: Disposable after-housing income scaled so the panel median equals 100 in each year.

@bruin */

WITH income_deduped AS (
    SELECT *
    FROM raw.oecd_tl2_income
    WHERE region_code IS NOT NULL
      AND year IS NOT NULL
      AND measure_code IN ('B5N', 'B6N')
      AND territorial_level = 'TL2'
      AND price_base = 'V'
      AND unit_measure = 'USD_PPP_PS'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY region_code, year, measure_code
        ORDER BY extracted_at DESC
    ) = 1
),

housing_deduped AS (
    SELECT *
    FROM raw.oecd_tl2_housing
    WHERE region_code IS NOT NULL
      AND year IS NOT NULL
      AND measure_code = 'HOUSE_COST'
      AND territorial_level = 'TL2'
      AND age = '_Z'
      AND sex = '_Z'
      AND unit_measure = 'PT_B6N_S14'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY region_code, year, measure_code
        ORDER BY extracted_at DESC
    ) = 1
),

income_wide AS (
    SELECT
        region_code,
        year,
        ANY_VALUE(region_name) AS region_name,
        ANY_VALUE(country_prefix) AS country_prefix,
        MAX(IF(measure_code = 'B5N', value, NULL)) AS primary_income_ppp_pc,
        MAX(IF(measure_code = 'B6N', value, NULL)) AS disposable_income_ppp_pc
    FROM income_deduped
    GROUP BY region_code, year
),

joined AS (
    SELECT
        i.region_code,
        i.year,
        i.region_name,
        i.country_prefix,
        CASE i.country_prefix
            WHEN 'AT' THEN 'Austria'
            WHEN 'BE' THEN 'Belgium'
            WHEN 'BG' THEN 'Bulgaria'
            WHEN 'CA' THEN 'Canada'
            WHEN 'CH' THEN 'Switzerland'
            WHEN 'CR' THEN 'Costa Rica'
            WHEN 'CZ' THEN 'Czechia'
            WHEN 'DE' THEN 'Germany'
            WHEN 'DK' THEN 'Denmark'
            WHEN 'EE' THEN 'Estonia'
            WHEN 'EL' THEN 'Greece'
            WHEN 'ES' THEN 'Spain'
            WHEN 'FI' THEN 'Finland'
            WHEN 'FR' THEN 'France'
            WHEN 'HR' THEN 'Croatia'
            WHEN 'HU' THEN 'Hungary'
            WHEN 'IE' THEN 'Ireland'
            WHEN 'IS' THEN 'Iceland'
            WHEN 'IT' THEN 'Italy'
            WHEN 'LT' THEN 'Lithuania'
            WHEN 'LV' THEN 'Latvia'
            WHEN 'ME' THEN 'Mexico'
            WHEN 'NL' THEN 'Netherlands'
            WHEN 'NO' THEN 'Norway'
            WHEN 'PL' THEN 'Poland'
            WHEN 'PT' THEN 'Portugal'
            WHEN 'SI' THEN 'Slovenia'
            WHEN 'SK' THEN 'Slovakia'
            WHEN 'TR' THEN 'Turkey'
        END AS country_name,
        CASE
            WHEN i.country_prefix IN ('CA', 'CR', 'ME') THEN 'North America'
            ELSE 'Europe'
        END AS macro_region,
        i.primary_income_ppp_pc,
        i.disposable_income_ppp_pc,
        h.value AS housing_cost_pct_disposable_income
    FROM income_wide i
    INNER JOIN housing_deduped h
        ON h.region_code = i.region_code
       AND h.year = i.year
    WHERE i.country_prefix IN (
        'AT', 'BE', 'BG', 'CA', 'CH', 'CR', 'CZ', 'DE', 'DK', 'EE', 'EL', 'ES',
        'FI', 'FR', 'HR', 'HU', 'IE', 'IS', 'IT', 'LT', 'LV', 'ME', 'NL', 'NO',
        'PL', 'PT', 'SI', 'SK', 'TR'
    )
      AND i.primary_income_ppp_pc IS NOT NULL
      AND i.disposable_income_ppp_pc IS NOT NULL
      AND h.value IS NOT NULL
),

scored AS (
    SELECT
        *,
        primary_income_ppp_pc - disposable_income_ppp_pc AS tax_transfer_gap_ppp_pc,
        SAFE_DIVIDE(primary_income_ppp_pc - disposable_income_ppp_pc, primary_income_ppp_pc) * 100 AS tax_transfer_gap_pct,
        disposable_income_ppp_pc * (1 - SAFE_DIVIDE(housing_cost_pct_disposable_income, 100)) AS disposable_after_housing_ppp_pc
    FROM joined
)

SELECT
    region_code,
    year,
    region_name,
    country_prefix,
    country_name,
    macro_region,
    primary_income_ppp_pc,
    disposable_income_ppp_pc,
    tax_transfer_gap_ppp_pc,
    tax_transfer_gap_pct,
    housing_cost_pct_disposable_income,
    disposable_after_housing_ppp_pc,
    SAFE_DIVIDE(
        disposable_after_housing_ppp_pc,
        PERCENTILE_CONT(disposable_after_housing_ppp_pc, 0.5) OVER (PARTITION BY year)
    ) * 100 AS affordability_score
FROM scored
ORDER BY year, country_name, region_name
