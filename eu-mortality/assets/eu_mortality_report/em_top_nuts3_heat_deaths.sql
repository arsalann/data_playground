/* @bruin

name: eu_mortality_report.em_top_nuts3_heat_deaths
type: bq.sql
description: |
  Analytical mart ranking EU-27 NUTS3 regions by total heat-attributable excess deaths (2015-2025).

  Aggregates heat attribution data from the climatological excess mortality model using 2015-2019
  baseline and +2°C temperature anomaly threshold (vs 1991-2020 climatology). Combines mortality
  statistics with regional demographic and geographic characteristics for comparative analysis.

  Powers the "deadliest regions" visualization in the EU environmental dashboard. Each row represents
  one NUTS3 region with 10-year totals, population context, and territorial classifications. Regions
  with no heat-attributable excess deaths are excluded. Ordered by total heat-attributable deaths
  descending for dashboard rendering efficiency.
connection: bruin-playground-arsalan
tags:
  - eu-27
  - mortality
  - report
  - nuts3
  - heat-attribution
  - public-health
  - climate-health
  - excess-mortality
  - temperature-analysis
  - demographic
  - geographic
  - mart
  - dashboard
  - eurostat
  - territorial-analysis

materialization:
  type: table
  strategy: create+replace

depends:
  - eu_mortality_staging.em_heat_attribution
  - eu_mortality_staging.em_nuts3_dim
  - eu_mortality_staging.em_population_dim

columns:
  - name: nuts_id
    type: VARCHAR
    description: NUTS3 region code (5-character identifier, e.g. 'FR101' for Paris). Primary identifier linking to Eurostat geographic hierarchy.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-2 country code. EU-27 member states only (AT, BE, BG, etc.).
    checks:
      - name: not_null
  - name: country_name_en
    type: VARCHAR
    description: Country name in English. Standardized Eurostat country labels.
    checks:
      - name: not_null
  - name: name_latn
    type: VARCHAR
    description: NUTS3 region name in Latin script. Official territorial designation from Eurostat NUTS classification.
    checks:
      - name: not_null
  - name: degurba_label
    type: VARCHAR
    description: Degree of Urbanisation classification. Categories include cities, towns & suburbs, and rural areas based on population density.
  - name: coast_label
    type: VARCHAR
    description: Coastal proximity classification. Binary indicator for regions with significant coastal territory.
  - name: mount_label
    type: VARCHAR
    description: Mountain region classification. Categorizes regions by topographic characteristics and elevation patterns.
  - name: heat_attributable_excess_total
    type: DOUBLE
    description: Total heat-attributable excess deaths across 2015-2025 (count). Only positive excess deaths during heat weeks (+2°C anomaly threshold) are included. Excludes non-heat excess mortality.
  - name: excess_deaths_total
    type: DOUBLE
    description: Total excess deaths 2015-2025 (count). All-cause mortality minus climatological baseline, including both positive and negative values across all temperature conditions.
  - name: heat_weeks_total
    type: INTEGER
    description: Number of heat weeks experienced by this region 2015-2025 (count). Heat weeks defined as ISO weeks with mean temperature >+2°C above 1991-2020 climatology.
  - name: avg_share_65plus
    type: DOUBLE
    description: Mean share of population aged 65 and older (proportion, 0-1 scale). Demographic vulnerability indicator averaged across 2015-2025 period.
  - name: avg_pop_total
    type: DOUBLE
    description: Mean total population 2015-2025 (persons). Denominator for rate calculations, sourced from Eurostat demographic data.
  - name: heat_deaths_per_100k
    type: DOUBLE
    description: Heat-attributable excess deaths per 100,000 inhabitants (rate). Population-standardized metric enabling comparison across regions of different sizes.

@bruin */

WITH agg AS (
    SELECT
        nuts_id,
        SUM(IFNULL(heat_attributable_excess, 0)) AS heat_attributable_excess_total,
        SUM(IFNULL(excess_deaths, 0)) AS excess_deaths_total,
        COUNTIF(is_heat_week) AS heat_weeks_total
    FROM `bruin-playground-arsalan.eu_mortality_staging.em_heat_attribution`
    GROUP BY nuts_id
),

pop_avg AS (
    SELECT
        nuts_id,
        AVG(pop_total) AS avg_pop_total,
        AVG(share_65plus) AS avg_share_65plus
    FROM `bruin-playground-arsalan.eu_mortality_staging.em_population_dim`
    WHERE ref_year BETWEEN 2015 AND 2025
    GROUP BY nuts_id
)

SELECT
    d.nuts_id,
    d.country_code,
    d.country_name_en,
    d.name_latn,
    d.degurba_label,
    d.coast_label,
    d.mount_label,
    a.heat_attributable_excess_total,
    a.excess_deaths_total,
    CAST(a.heat_weeks_total AS INT64) AS heat_weeks_total,
    p.avg_share_65plus,
    p.avg_pop_total,
    SAFE_DIVIDE(a.heat_attributable_excess_total, p.avg_pop_total) * 100000 AS heat_deaths_per_100k
FROM `bruin-playground-arsalan.eu_mortality_staging.em_nuts3_dim` d
LEFT JOIN agg a USING (nuts_id)
LEFT JOIN pop_avg p USING (nuts_id)
WHERE a.heat_attributable_excess_total IS NOT NULL
ORDER BY a.heat_attributable_excess_total DESC NULLS LAST
