/* @bruin
name: report.regional_affordability_snapshot
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Latest high-coverage apples-to-apples OECD TL2 affordability snapshot used
  by the DAC dashboard and choropleth map. The selected year is 2022 because it
  is the latest year with broad region coverage across all three required
  measures: primary income, disposable income, and housing-cost burden.

depends:
  - staging.regional_affordability

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
    description: Snapshot year; fixed to 2022 for this dashboard.
  - name: region_name
    type: VARCHAR
    description: Human-readable OECD TL2 region name.
  - name: country_name
    type: VARCHAR
    description: Country name normalized from the OECD regional code prefix.
  - name: macro_region
    type: VARCHAR
    description: Europe or North America.
  - name: primary_income_ppp_pc
    type: DOUBLE
    description: Net primary income per person in current-price USD PPP.
  - name: disposable_income_ppp_pc
    type: DOUBLE
    description: Net disposable income per person in current-price USD PPP.
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
    description: Disposable after-housing income scaled so the 2022 panel median equals 100.

@bruin */

SELECT
    region_code,
    year,
    region_name,
    country_name,
    macro_region,
    primary_income_ppp_pc,
    disposable_income_ppp_pc,
    tax_transfer_gap_pct,
    housing_cost_pct_disposable_income,
    disposable_after_housing_ppp_pc,
    affordability_score
FROM staging.regional_affordability
WHERE year = 2022
ORDER BY affordability_score DESC
