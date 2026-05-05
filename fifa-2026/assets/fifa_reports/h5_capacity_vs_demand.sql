/* @bruin

name: fifa_reports.h5_capacity_vs_demand
type: bq.sql
description: |
  H5 — Stadium capacity vs. expected demand. One row per match (104).

  demand_score = log10(metro_pop_m * 1e6) + 0.5 * team_interest_max
  with team_interest currently a flat placeholder of 50 (Google-Trends asset
  deferred — flagged in dashboard footnote and AGENTS.md). Once the trends
  asset is wired up, swap the placeholder for the per-match max of the two
  participating teams' Google Trends interest in the host country.

  capacity_minus_demand_z is the z-score of (log10(capacity) - demand_score)
  across all 104 fixtures. Negative values flag potential under-subscription
  (capacity below what demand would predict); positive values flag
  over-subscription (capacity exceeds demand).
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - reports
  - h5

materialization:
  type: table
  strategy: create+replace

depends:
  - fifa_staging.stadium_demand_inputs

@bruin */

WITH base AS (
  SELECT
    match_id,
    stage,
    venue_id,
    venue_city,
    stadium,
    venue_capacity,
    kickoff_local,
    metro_pop_m,
    gdp_per_capita_usd,
    team_interest_placeholder,
    /* H5 demand-score heuristic — see staging description. */
    CASE
      WHEN metro_pop_m IS NULL THEN NULL
      ELSE LOG10(metro_pop_m * 1e6) + 0.5 * team_interest_placeholder
    END AS demand_score,
    LOG10(venue_capacity) AS log_capacity
  FROM `bruin-playground-arsalan.fifa_staging.stadium_demand_inputs`
),
stats AS (
  SELECT
    AVG(log_capacity - demand_score)         AS mu_gap,
    STDDEV_SAMP(log_capacity - demand_score) AS sd_gap
  FROM base
  WHERE demand_score IS NOT NULL
)
SELECT
  b.match_id,
  b.stage,
  b.venue_city,
  b.stadium,
  b.venue_capacity,
  b.kickoff_local,
  ROUND(b.metro_pop_m,  2)                                 AS metro_pop_m,
  b.team_interest_placeholder,
  ROUND(b.demand_score, 3)                                 AS demand_score,
  ROUND(b.log_capacity, 3)                                 AS log_capacity,
  ROUND(b.log_capacity - b.demand_score, 3)                AS capacity_minus_demand,
  ROUND(
    ((b.log_capacity - b.demand_score) - s.mu_gap) / NULLIF(s.sd_gap, 0),
    3
  )                                                        AS capacity_minus_demand_z,
  CASE
    WHEN b.demand_score IS NULL THEN 'no_data'
    WHEN ((b.log_capacity - b.demand_score) - s.mu_gap) / NULLIF(s.sd_gap, 0) >  1
      THEN 'over_supplied'
    WHEN ((b.log_capacity - b.demand_score) - s.mu_gap) / NULLIF(s.sd_gap, 0) < -1
      THEN 'under_supplied'
    ELSE 'aligned'
  END                                                      AS flag,
  CAST(
    'team_interest_max is a flat 50 placeholder (Google Trends asset deferred). '
    'demand_score = log10(metro_pop_m * 1e6) + 0.5 * team_interest_max.'
    AS STRING
  ) AS methodology_note,
  CURRENT_TIMESTAMP() AS reported_at
FROM base b, stats s
ORDER BY capacity_minus_demand_z
