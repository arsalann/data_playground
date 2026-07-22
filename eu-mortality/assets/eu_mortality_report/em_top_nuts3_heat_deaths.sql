/* @bruin

name: eu_mortality_report.em_top_nuts3_heat_deaths
type: bq.sql
description: |
  Top NUTS3 regions ranked by total heat-attributable excess deaths across
  2015 - 2025. Mart for the dashboard "deadliest regions" chart.

  Per row: NUTS3 identifier, total heat-attributable excess deaths over the full
  10-year window, total excess deaths regardless of heat attribution, average
  share of population aged 65 and over, DEGURBA + coast + mountain
  classifications.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - eu_mortality_staging.em_heat_attribution
  - eu_mortality_staging.em_nuts3_dim
  - eu_mortality_staging.em_population_dim

tags:
  - eu-27
  - mortality
  - report
  - nuts3
  - heat-attribution

columns:
  - name: nuts_id
    type: VARCHAR
    description: NUTS3 region code.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-2.
  - name: country_name_en
    type: VARCHAR
    description: English country name.
  - name: name_latn
    type: VARCHAR
    description: Latin regional name.
  - name: degurba_label
    type: VARCHAR
    description: DEGURBA classification.
  - name: coast_label
    type: VARCHAR
    description: Coastal class.
  - name: mount_label
    type: VARCHAR
    description: Mountain class.
  - name: heat_attributable_excess_total
    type: DOUBLE
    description: Sum of heat-attributable excess deaths 2015-2025.
  - name: excess_deaths_total
    type: DOUBLE
    description: Sum of excess deaths (positive and negative) 2015-2025.
  - name: heat_weeks_total
    type: INTEGER
    description: NUTS3 x ISO-week count of heat weeks.
  - name: avg_share_65plus
    type: DOUBLE
    description: Mean share of population aged 65+ across the window.
  - name: avg_pop_total
    type: DOUBLE
    description: Mean total population across the window.
  - name: heat_deaths_per_100k
    type: DOUBLE
    description: Heat-attributable excess deaths per 100,000 inhabitants (heat_attributable_excess_total / avg_pop_total * 100000).

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
