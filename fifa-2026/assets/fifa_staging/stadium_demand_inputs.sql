/* @bruin

name: fifa_staging.stadium_demand_inputs
type: bq.sql
description: |
  Per-fixture inputs to the H5 demand-score heuristic: stadium capacity,
  metro-area population, and (placeholder) team interest signal.

  Demand score (computed in the report layer):
      demand_score = log10(metro_pop_m * 1e6) + 0.5 * team_interest_max
  where team_interest_max is the higher of the two participating teams'
  Google-Trends signal (currently a flat 50 placeholder until the
  google_trends_team_interest asset is added — see AGENTS.md).

  H5 reports under/over-subscription as standardised gap between
  log-capacity and demand_score.
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - staging
  - demand

materialization:
  type: table
  strategy: create+replace

depends:
  - fifa_staging.matches_enriched
  - fifa_raw.city_demographics

@bruin */

SELECT
  m.match_id,
  m.stage,
  m.venue_id,
  m.venue_city,
  m.stadium,
  m.venue_capacity,
  m.kickoff_local,
  d.metro_pop_m,
  d.gdp_per_capita_usd,
  CAST(50 AS INT64) AS team_interest_placeholder,
  CURRENT_TIMESTAMP() AS staged_at
FROM `bruin-playground-arsalan.fifa_staging.matches_enriched` m
LEFT JOIN `bruin-playground-arsalan.fifa_raw.city_demographics` d
  ON d.city = m.venue_city
