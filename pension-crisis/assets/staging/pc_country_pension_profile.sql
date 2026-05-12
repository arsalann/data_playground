/* @bruin

name: staging.pc_country_pension_profile
type: bq.sql
description: |
  Mart-style per-country pension profile. One row per OECD country joining:
  - Pension-system indicators (staging.pc_pension_system)
  - Mercer 2025 scores (staging.pc_mercer_scores)
  - Old-age dependency today (latest observed year, typically 2024) and 2050 projection
    from UN WPP 2024
  - Life expectancy at 65 today and 2050 projection from UN WPP 2024

  The "years in retirement" derived column is computed as life_expectancy_at_65_today
  minus (retirement_age_latest - 65) to answer: how many years does the average
  worker spend retired? Only meaningful where both inputs are non-null.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - staging.pc_country_dim
  - staging.pc_pension_system
  - staging.pc_mercer_scores
  - staging.pc_demographics_annual
  - raw.pc_imf_public_debt
  - raw.pc_oecd_old_age_poverty

columns:
  - name: iso3_code
    type: VARCHAR
    description: ISO-3 country code.
    primary_key: true
    nullable: false
  - name: country_name
    type: VARCHAR
    description: Country name.
  - name: region
    type: VARCHAR
    description: Geographic region.
  - name: retirement_age_latest
    type: DOUBLE
    description: Latest statutory retirement age.
  - name: net_replacement_rate_latest
    type: DOUBLE
    description: Latest net pension replacement rate (%).
  - name: pension_spending_pct_gdp_latest
    type: DOUBLE
    description: Latest public pension spending % GDP.
  - name: pension_assets_pct_gdp_latest
    type: DOUBLE
    description: Latest pension fund assets % GDP.
  - name: mercer_overall
    type: DOUBLE
    description: Mercer GPI overall score.
  - name: mercer_adequacy
    type: DOUBLE
    description: Mercer adequacy sub-index.
  - name: mercer_sustainability
    type: DOUBLE
    description: Mercer sustainability sub-index.
  - name: mercer_integrity
    type: DOUBLE
    description: Mercer integrity sub-index.
  - name: old_age_dep_ratio_today
    type: DOUBLE
    description: Old-age dependency ratio in latest observed year (UN WPP).
  - name: old_age_dep_ratio_2050
    type: DOUBLE
    description: Old-age dependency ratio in 2050 (UN WPP medium variant).
  - name: life_expectancy_at_65_today
    type: DOUBLE
    description: Life expectancy at 65 in latest observed year (UN WPP).
  - name: life_expectancy_at_65_2050
    type: DOUBLE
    description: Life expectancy at 65 in 2050 (UN WPP medium variant).
  - name: years_in_retirement_today
    type: DOUBLE
    description: life_expectancy_at_65_today - (retirement_age_latest - 65). Derived.
  - name: public_debt_pct_gdp_latest
    type: DOUBLE
    description: IMF general-government gross debt as % of GDP, latest non-forecast year.
  - name: public_debt_pct_gdp_year
    type: INTEGER
    description: Year of the public_debt_pct_gdp_latest observation.
  - name: public_debt_pct_gdp_2030
    type: DOUBLE
    description: IMF WEO forecast of general-government gross debt as % of GDP in 2030.
  - name: old_age_poverty_rate_latest
    type: DOUBLE
    description: OECD IDD poverty rate for persons 65+, 50%-of-median disposable income, latest year.
  - name: old_age_poverty_rate_year
    type: INTEGER
    description: Year of the old_age_poverty_rate_latest observation.

@bruin */

WITH latest_year AS (
    SELECT MAX(year) AS max_observed_year
    FROM `bruin-playground-arsalan.staging.pc_demographics_annual`
    WHERE NOT is_projection
),

demo_today AS (
    SELECT
        d.iso3_code,
        d.old_age_dep_ratio AS old_age_dep_ratio_today,
        d.life_expectancy_at_65 AS life_expectancy_at_65_today
    FROM `bruin-playground-arsalan.staging.pc_demographics_annual` d, latest_year l
    WHERE d.year = l.max_observed_year
),

demo_2050 AS (
    SELECT
        iso3_code,
        old_age_dep_ratio AS old_age_dep_ratio_2050,
        life_expectancy_at_65 AS life_expectancy_at_65_2050
    FROM `bruin-playground-arsalan.staging.pc_demographics_annual`
    WHERE year = 2050
),

debt_latest AS (
    SELECT
        iso3_code,
        debt_pct_gdp AS public_debt_pct_gdp_latest,
        year AS public_debt_pct_gdp_year
    FROM (
        SELECT
            iso3_code,
            debt_pct_gdp,
            year,
            ROW_NUMBER() OVER (PARTITION BY iso3_code ORDER BY year DESC) AS rn
        FROM `bruin-playground-arsalan.raw.pc_imf_public_debt`
        WHERE NOT is_forecast
    )
    WHERE rn = 1
),

debt_2030 AS (
    SELECT
        iso3_code,
        debt_pct_gdp AS public_debt_pct_gdp_2030
    FROM `bruin-playground-arsalan.raw.pc_imf_public_debt`
    WHERE year = 2030
),

poverty_latest AS (
    SELECT
        iso3_code,
        poverty_rate AS old_age_poverty_rate_latest,
        year AS old_age_poverty_rate_year
    FROM (
        SELECT
            iso3_code,
            poverty_rate,
            year,
            ROW_NUMBER() OVER (PARTITION BY iso3_code ORDER BY year DESC) AS rn
        FROM `bruin-playground-arsalan.raw.pc_oecd_old_age_poverty`
    )
    WHERE rn = 1
)

SELECT
    c.iso3_code,
    c.country_name,
    c.region,
    p.retirement_age_latest,
    p.net_replacement_rate_latest,
    p.pension_spending_pct_gdp_latest,
    p.pension_assets_pct_gdp_latest,
    m.overall_index AS mercer_overall,
    m.adequacy_sub_index AS mercer_adequacy,
    m.sustainability_sub_index AS mercer_sustainability,
    m.integrity_sub_index AS mercer_integrity,
    dt.old_age_dep_ratio_today,
    d50.old_age_dep_ratio_2050,
    dt.life_expectancy_at_65_today,
    d50.life_expectancy_at_65_2050,
    CASE
        WHEN dt.life_expectancy_at_65_today IS NULL OR p.retirement_age_latest IS NULL THEN NULL
        ELSE dt.life_expectancy_at_65_today - (p.retirement_age_latest - 65)
    END AS years_in_retirement_today,
    dl.public_debt_pct_gdp_latest,
    dl.public_debt_pct_gdp_year,
    d30.public_debt_pct_gdp_2030,
    pl.old_age_poverty_rate_latest,
    pl.old_age_poverty_rate_year
FROM `bruin-playground-arsalan.staging.pc_country_dim` c
LEFT JOIN `bruin-playground-arsalan.staging.pc_pension_system` p USING (iso3_code)
LEFT JOIN `bruin-playground-arsalan.staging.pc_mercer_scores` m USING (iso3_code)
LEFT JOIN demo_today dt USING (iso3_code)
LEFT JOIN demo_2050 d50 USING (iso3_code)
LEFT JOIN debt_latest dl USING (iso3_code)
LEFT JOIN debt_2030 d30 USING (iso3_code)
LEFT JOIN poverty_latest pl USING (iso3_code)
ORDER BY c.iso3_code
