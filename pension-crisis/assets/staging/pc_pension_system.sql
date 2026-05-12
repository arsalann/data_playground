/* @bruin

name: staging.pc_pension_system
type: bq.sql
description: |
  One row per OECD country summarising the pension system using a consistent,
  apples-to-apples filter for each indicator. Rather than picking the "latest
  year" per country (which can silently vary from 2019 to 2024 across countries),
  this query pins each indicator to a single reference year chosen to maximise
  OECD-38 coverage.

  Harmonisation rules:
  - Normal statutory retirement age: MEASURE=CRPLF22 (Current Retirement Pension
    Length-of-service Full-career, aged 22 entry), average of M and F where both
    are reported. Reference year = 2024.
  - Net pension replacement rate: MEASURE=NPRR100 (100% of average earnings),
    OPTIONALITY=M (mandatory scheme only — avoids double-counting voluntary
    contributions which differ widely by country). Average of M and F. Reference
    year = 2024.
  - Public pension spending %GDP: MEASURE=PEP in the PAG bundle. Reference year =
    2021 (latest year with all 38 OECD countries reporting).
  - Pension fund assets %GDP: VEHICLE_TYPE=_T (all financing vehicles combined:
    pension funds + insurance contracts + banks + investment companies + other).
    Reference year = 2023 (latest with broad coverage).
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - raw.pc_oecd_retirement_age
  - raw.pc_oecd_replacement_rate
  - raw.pc_oecd_pension_spending
  - raw.pc_oecd_pension_assets
  - staging.pc_country_dim

columns:
  - name: iso3_code
    type: VARCHAR
    description: ISO-3 country code.
    primary_key: true
    nullable: false
  - name: country_name
    type: VARCHAR
    description: Canonical country name.
  - name: retirement_age_latest
    type: DOUBLE
    description: Current normal statutory retirement age (years), average of M and F.
  - name: retirement_age_year
    type: INTEGER
    description: Reference year of the retirement age observation (pinned to 2024).
  - name: net_replacement_rate_latest
    type: DOUBLE
    description: Net pension replacement rate (%) at average earnings, mandatory scheme only, average of M and F.
  - name: replacement_rate_year
    type: INTEGER
    description: Reference year (pinned to 2024).
  - name: pension_spending_pct_gdp_latest
    type: DOUBLE
    description: Public pension spending as % of GDP (MEASURE=PEP). Reference year = 2021.
  - name: pension_spending_year
    type: INTEGER
    description: Reference year (pinned to 2021).
  - name: pension_assets_pct_gdp_latest
    type: DOUBLE
    description: Pension-type assets as % of GDP, all financing vehicles combined. Reference year = 2023.
  - name: pension_assets_year
    type: INTEGER
    description: Reference year (pinned to 2023).

@bruin */

WITH retirement_age AS (
    SELECT
        iso3_code,
        2024 AS retirement_age_year,
        AVG(retirement_age) AS retirement_age_latest
    FROM `bruin-playground-arsalan.raw.pc_oecd_retirement_age`
    WHERE indicator_code = 'CRPLF22'
      AND year = 2024
      AND retirement_age IS NOT NULL
    GROUP BY iso3_code
),

replacement_rate AS (
    SELECT
        iso3_code,
        2024 AS replacement_rate_year,
        AVG(net_replacement_rate) AS net_replacement_rate_latest
    FROM `bruin-playground-arsalan.raw.pc_oecd_replacement_rate`
    WHERE measure_code = 'NPRR100'
      AND optionality = 'M'
      AND year = 2024
      AND net_replacement_rate IS NOT NULL
    GROUP BY iso3_code
),

spending AS (
    SELECT
        iso3_code,
        2021 AS pension_spending_year,
        MAX(spending_pct_gdp) AS pension_spending_pct_gdp_latest
    FROM `bruin-playground-arsalan.raw.pc_oecd_pension_spending`
    WHERE year = 2021
      AND spending_pct_gdp IS NOT NULL
    GROUP BY iso3_code
),

assets AS (
    SELECT
        iso3_code,
        2023 AS pension_assets_year,
        MAX(assets_pct_gdp) AS pension_assets_pct_gdp_latest
    FROM `bruin-playground-arsalan.raw.pc_oecd_pension_assets`
    WHERE vehicle_type = '_T'
      AND year = 2023
      AND assets_pct_gdp IS NOT NULL
    GROUP BY iso3_code
)

SELECT
    d.iso3_code,
    d.country_name,
    r.retirement_age_latest,
    r.retirement_age_year,
    rr.net_replacement_rate_latest,
    rr.replacement_rate_year,
    s.pension_spending_pct_gdp_latest,
    s.pension_spending_year,
    a.pension_assets_pct_gdp_latest,
    a.pension_assets_year
FROM `bruin-playground-arsalan.staging.pc_country_dim` d
LEFT JOIN retirement_age r USING (iso3_code)
LEFT JOIN replacement_rate rr USING (iso3_code)
LEFT JOIN spending s USING (iso3_code)
LEFT JOIN assets a USING (iso3_code)
ORDER BY d.iso3_code
