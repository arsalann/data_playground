/* @bruin

name: fifa_reports.r2_travel_summary
type: bq.sql
description: |
  R2 — Headline travel-inequality metrics. One row, several scalar fields:
    - max_team_km / min_team_km / ratio
    - mean_team_km / median_team_km
    - total_team_km (sum across all 48 teams)
    - n_teams_over_15000km / n_teams_under_5000km

  Used as the "lottery" headline in the dashboard. The ratio max/min is the
  story: the unluckiest schedule team flies multiple times what the luckiest
  team flies, before any team wins or loses a single match.
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - reports
  - r2

materialization:
  type: table
  strategy: create+replace

depends:
  - fifa_reports.h2_team_travel_burden

@bruin */

WITH per_team AS (
  /* Some teams are duplicated by group_id when joined; aggregate to one row per team. */
  SELECT
    fifa_code,
    MAX(total_km)            AS total_km,
    MAX(altitude_m_gained)   AS altitude_m_gained,
    MAX(max_tz_shift_h)      AS max_tz_shift_h
  FROM `bruin-playground-arsalan.fifa_reports.h2_team_travel_burden`
  GROUP BY fifa_code
)
SELECT
  COUNT(*)                                                AS n_teams,
  ROUND(MAX(total_km), 0)                                 AS max_team_km,
  ROUND(MIN(total_km), 0)                                 AS min_team_km,
  ROUND(SAFE_DIVIDE(MAX(total_km), MIN(total_km)), 2)     AS km_max_min_ratio,
  ROUND(AVG(total_km), 0)                                 AS mean_team_km,
  ROUND(APPROX_QUANTILES(total_km, 100)[OFFSET(50)], 0)   AS median_team_km,
  ROUND(SUM(total_km), 0)                                 AS total_team_km,
  COUNTIF(total_km >= 15000)                              AS n_teams_over_15000km,
  COUNTIF(total_km <  5000)                               AS n_teams_under_5000km,
  ROUND(MAX(altitude_m_gained), 0)                        AS max_altitude_gained_m,
  ROUND(MAX(max_tz_shift_h), 1)                           AS max_tz_shift_h,
  CURRENT_TIMESTAMP()                                     AS reported_at
FROM per_team
