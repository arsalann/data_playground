/* @bruin
name: report.atlas_yearly_trend
type: bq.sql
connection: bruin-playground-arsalan
description: |
  One row per summer season (2019-2026): population (millions, GHS-POP 2015, held
  fixed so changes reflect hazards, not growth) of 1M+ cities meeting each
  season-level hazard rule, and the share of each HDI tier's population exposed
  to two or more hazards.

depends:
  - staging.city_exposure

materialization:
  type: table
  strategy: create+replace

columns:
  - name: season_year
    type: INTEGER
    description: Summer season (calendar year).
    primary_key: true
  - name: season_label
    type: VARCHAR
    description: Season as a string, for a categorical x-axis.
  - name: heat_pop_m
    type: DOUBLE
    description: Population (millions) in heat-exposed cities.
  - name: fire_pop_m
    type: DOUBLE
    description: Population (millions) in fire-exposed cities.
  - name: flood_pop_m
    type: DOUBLE
    description: Population (millions) in flood-exposed cities.
  - name: two_plus_pop_m
    type: DOUBLE
    description: Population (millions) in cities exposed to 2 or 3 hazards.
  - name: two_plus_cities
    type: INTEGER
    description: Number of cities exposed to 2 or 3 hazards.
  - name: low_hdi_two_plus_pct
    type: DOUBLE
    description: Share of Low-HDI city population exposed to 2+ hazards (%).
  - name: medium_hdi_two_plus_pct
    type: DOUBLE
    description: Share of Medium-HDI city population exposed to 2+ hazards (%).
  - name: high_hdi_two_plus_pct
    type: DOUBLE
    description: Share of High-HDI city population exposed to 2+ hazards (%).
  - name: very_high_hdi_two_plus_pct
    type: DOUBLE
    description: Share of Very-high-HDI city population exposed to 2+ hazards (%).
@bruin */

WITH tier_share AS (
    SELECT
        season_year,
        hdi_tier,
        100 * SUM(IF(hazard_count >= 2, population_2015, 0)) / SUM(population_2015) AS two_plus_pct
    FROM staging.city_exposure
    WHERE hdi_tier != 'Unknown'
    GROUP BY 1, 2
)

SELECT
    e.season_year,
    CAST(e.season_year AS STRING) AS season_label,
    ROUND(SUM(IF(e.is_heat_exposed, e.population_2015, 0)) / 1e6, 1) AS heat_pop_m,
    ROUND(SUM(IF(e.is_fire_exposed, e.population_2015, 0)) / 1e6, 1) AS fire_pop_m,
    ROUND(SUM(IF(e.is_flood_exposed, e.population_2015, 0)) / 1e6, 1) AS flood_pop_m,
    ROUND(SUM(IF(e.hazard_count >= 2, e.population_2015, 0)) / 1e6, 1) AS two_plus_pop_m,
    COUNTIF(e.hazard_count >= 2) AS two_plus_cities,
    ROUND(ANY_VALUE(t.low), 1) AS low_hdi_two_plus_pct,
    ROUND(ANY_VALUE(t.medium), 1) AS medium_hdi_two_plus_pct,
    ROUND(ANY_VALUE(t.high), 1) AS high_hdi_two_plus_pct,
    ROUND(ANY_VALUE(t.very_high), 1) AS very_high_hdi_two_plus_pct
FROM staging.city_exposure AS e
LEFT JOIN (
    SELECT
        season_year,
        MAX(IF(hdi_tier = 'Low', two_plus_pct, NULL)) AS low,
        MAX(IF(hdi_tier = 'Medium', two_plus_pct, NULL)) AS medium,
        MAX(IF(hdi_tier = 'High', two_plus_pct, NULL)) AS high,
        MAX(IF(hdi_tier = 'Very high', two_plus_pct, NULL)) AS very_high
    FROM tier_share
    GROUP BY 1
) AS t USING (season_year)
GROUP BY 1, 2
ORDER BY 1
