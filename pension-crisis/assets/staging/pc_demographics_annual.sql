/* @bruin

name: staging.pc_demographics_annual
type: bq.sql
description: |
  Country-year demographic panel combining UN WPP 2024 population-by-age with life
  expectancy at 65 from the UN WPP 2024 abridged life tables. Scope: 38 OECD
  countries, medium-variant projections, years 1950-2100.

  Single UN-DESA methodology applied uniformly — cleanest apples-to-apples demography
  available.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - raw.pc_un_wpp_population
  - raw.pc_un_wpp_life_expectancy
  - staging.pc_country_dim

columns:
  - name: iso3_code
    type: VARCHAR
    description: ISO 3166-1 alpha-3 country code.
    primary_key: true
    nullable: false
  - name: year
    type: INTEGER
    description: Calendar year (1950-2100).
    primary_key: true
    nullable: false
  - name: country_name
    type: VARCHAR
    description: Canonical OECD country name.
  - name: pop_total
    type: DOUBLE
    description: Total population mid-year, thousands.
  - name: pop_15_64
    type: DOUBLE
    description: Working-age population 15-64, thousands.
  - name: pop_65plus
    type: DOUBLE
    description: Population aged 65+, thousands.
  - name: old_age_dep_ratio
    type: DOUBLE
    description: Old-age dependency ratio = pop_65plus / pop_15_64 * 100.
  - name: life_expectancy_at_65
    type: DOUBLE
    description: UN WPP 2024 remaining life expectancy at exact age 65, both sexes, years.
  - name: is_projection
    type: BOOLEAN
    description: TRUE for years after 2024 (medium-variant projection).

@bruin */

WITH pop_deduped AS (
    SELECT
        iso3_code,
        year,
        country_name,
        pop_total,
        pop_15_64,
        pop_65plus,
        old_age_dep_ratio
    FROM `bruin-playground-arsalan.raw.pc_un_wpp_population`
    WHERE iso3_code IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY iso3_code, year ORDER BY extracted_at DESC) = 1
),

e65_deduped AS (
    SELECT
        iso3_code,
        year,
        life_expectancy_at_65
    FROM `bruin-playground-arsalan.raw.pc_un_wpp_life_expectancy`
    WHERE iso3_code IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY iso3_code, year ORDER BY extracted_at DESC) = 1
)

SELECT
    d.iso3_code,
    p.year,
    d.country_name,
    p.pop_total,
    p.pop_15_64,
    p.pop_65plus,
    p.old_age_dep_ratio,
    e.life_expectancy_at_65,
    p.year > 2024 AS is_projection
FROM `bruin-playground-arsalan.staging.pc_country_dim` d
JOIN pop_deduped p USING (iso3_code)
LEFT JOIN e65_deduped e ON e.iso3_code = p.iso3_code AND e.year = p.year
ORDER BY d.iso3_code, p.year
