/* @bruin

name: fifa_staging.altitude_match_panel
type: bq.sql
description: |
  Historical World Cup matches (2010-2022) classified into altitude bands and
  decade buckets. One row per match. Drives H4 (altitude effect on scoring).

  Altitude bands:
    - Low      : <= 500 m
    - Moderate : 500 - 1500 m
    - High     : 1500 - 2200 m
    - Extreme  : > 2200 m

  Mexico City Azteca (2240m) and Guadalajara Akron (1566m) — the two FIFA-2026
  altitude venues — fall into Extreme and High respectively. The panel's role
  is to estimate goals/match in those bands using prior World Cup data, which
  H4 then uses to project the FIFA-2026 altitude-venue scoring premium.

  Liga MX inclusion was deferred (licensing); only WC data is in this build.
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - staging
  - historical

materialization:
  type: table
  strategy: create+replace

depends:
  - fifa_raw.historical_wc_matches

@bruin */

SELECT
  tournament,
  host,
  venue_city,
  home,
  away,
  stage,
  elevation_m,
  home_goals + away_goals  AS goals_total,
  home_goals,
  away_goals,
  CASE
    WHEN elevation_m <= 500  THEN 'Low'
    WHEN elevation_m <= 1500 THEN 'Moderate'
    WHEN elevation_m <= 2200 THEN 'High'
    ELSE 'Extreme'
  END AS altitude_band,
  CASE
    WHEN tournament BETWEEN 2010 AND 2014 THEN '2010s_early'
    WHEN tournament BETWEEN 2018 AND 2022 THEN '2010s_late_2020s'
    ELSE 'other'
  END AS era_bucket,
  'WC' AS source,
  CURRENT_TIMESTAMP() AS staged_at
FROM `bruin-playground-arsalan.fifa_raw.historical_wc_matches`
