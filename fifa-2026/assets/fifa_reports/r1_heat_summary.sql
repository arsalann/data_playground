/* @bruin

name: fifa_reports.r1_heat_summary
type: bq.sql
description: |
  R1 — Headline heat-exposure metrics for the dashboard scorecard.
  One row, several scalar fields:
    - n_matches_total: 104
    - n_open_air_effective: matches a player meaningfully experiences open-air heat
    - n_caution_or_worse_open_air: matches with apparent temp >= 27 °C and open-air-effective
    - n_extreme_caution_or_worse_open_air: matches with apparent temp >= 32 °C and open-air-effective
    - max_apparent_temp_c: hottest expected match (open-air only)
    - mean_apparent_warming_c: average across the 16 venues of (2015-2024) - (1980s)
      apparent-temp afternoon means
    - hottest_venue_warming: max delta venue, label and °C
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - reports
  - r1

materialization:
  type: table
  strategy: create+replace

depends:
  - fifa_reports.h1_match_heat_risk
  - fifa_reports.r1_venue_warming

@bruin */

WITH heat AS (
  SELECT
    COUNT(*)                                                              AS n_matches_total,
    SUM(IF(open_air_effective, 1, 0))                                     AS n_open_air_effective,
    SUM(IF(open_air_effective AND expected_apparent_temp_c >= 27, 1, 0))  AS n_caution_open_air,
    SUM(IF(open_air_effective AND expected_apparent_temp_c >= 32, 1, 0))  AS n_extreme_caution_open_air,
    SUM(IF(open_air_effective AND expected_apparent_temp_c >= 39, 1, 0))  AS n_danger_open_air,
    MAX(IF(open_air_effective, expected_apparent_temp_c, NULL))           AS max_apparent_temp_open_air_c
  FROM `bruin-playground-arsalan.fifa_reports.h1_match_heat_risk`
),
warm AS (
  SELECT
    AVG(apparent_warming_c)                                                AS mean_apparent_warming_c,
    AVG(warming_c)                                                         AS mean_temp_warming_c,
    ARRAY_AGG(STRUCT(venue_city, apparent_warming_c)
              ORDER BY apparent_warming_c DESC LIMIT 1)[OFFSET(0)]         AS hottest_warmer
  FROM `bruin-playground-arsalan.fifa_reports.r1_venue_warming`
)
SELECT
  h.n_matches_total,
  h.n_open_air_effective,
  h.n_caution_open_air,
  h.n_extreme_caution_open_air,
  h.n_danger_open_air,
  ROUND(h.max_apparent_temp_open_air_c, 2)        AS max_apparent_temp_open_air_c,
  ROUND(w.mean_apparent_warming_c, 2)             AS mean_apparent_warming_c,
  ROUND(w.mean_temp_warming_c, 2)                 AS mean_temp_warming_c,
  w.hottest_warmer.venue_city                     AS most_warmed_city,
  ROUND(w.hottest_warmer.apparent_warming_c, 2)   AS most_warmed_apparent_c,
  CURRENT_TIMESTAMP()                             AS reported_at
FROM heat h, warm w
