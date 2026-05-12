/* @bruin

name: fifa_reports.r1_venue_warming
type: bq.sql
description: |
  R1 — Per-venue decadal warming surface. One row per venue (16) with the
  1980s baseline and 2015-2024 recent-decade afternoon (12-18 venue-local)
  June-July apparent-temperature mean, the absolute °C warming, and the
  share of afternoon hours hot enough to be classed Caution or worse.

  Drives the "hottest World Cup ever played" narrative: the 16 venues are
  on average X °C hotter in afternoons now than they were a generation ago,
  and Y of the 104 fixtures are scheduled at venues with the steepest warming.
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - reports
  - r1
  - climate

materialization:
  type: table
  strategy: create+replace

depends:
  - fifa_staging.venue_decadal_warming
  - fifa_staging.matches_enriched

@bruin */

WITH wide AS (
  SELECT
    venue_id,
    venue_city,
    venue_country,
    venue_lat,
    venue_lon,
    MAX(IF(decade = '1980s',     mean_apparent_temp_c, NULL)) AS apparent_1980s,
    MAX(IF(decade = '2015-2024', mean_apparent_temp_c, NULL)) AS apparent_2015_2024,
    MAX(IF(decade = '1980s',     mean_temp_c, NULL))          AS temp_1980s,
    MAX(IF(decade = '2015-2024', mean_temp_c, NULL))          AS temp_2015_2024,
    MAX(IF(decade = '1980s',     pct_hours_ge30c, NULL))      AS pct_ge30_1980s,
    MAX(IF(decade = '2015-2024', pct_hours_ge30c, NULL))      AS pct_ge30_2015_2024,
    MAX(IF(decade = '1980s',     pct_hours_ge35c, NULL))      AS pct_ge35_1980s,
    MAX(IF(decade = '2015-2024', pct_hours_ge35c, NULL))      AS pct_ge35_2015_2024
  FROM `bruin-playground-arsalan.fifa_staging.venue_decadal_warming`
  GROUP BY venue_id, venue_city, venue_country, venue_lat, venue_lon
),
match_counts AS (
  SELECT
    venue_id,
    COUNT(*)                                       AS n_matches_2026,
    SUM(IF(stage = 'G', 1, 0))                     AS n_group_matches,
    SUM(IF(stage <> 'G', 1, 0))                    AS n_ko_matches
  FROM `bruin-playground-arsalan.fifa_staging.matches_enriched`
  GROUP BY venue_id
)
SELECT
  w.venue_id,
  w.venue_city,
  w.venue_country,
  w.venue_lat,
  w.venue_lon,
  ROUND(w.temp_1980s,            2) AS temp_1980s_c,
  ROUND(w.temp_2015_2024,        2) AS temp_2015_2024_c,
  ROUND(w.temp_2015_2024 - w.temp_1980s, 2) AS warming_c,
  ROUND(w.apparent_1980s,        2) AS apparent_1980s_c,
  ROUND(w.apparent_2015_2024,    2) AS apparent_2015_2024_c,
  ROUND(w.apparent_2015_2024 - w.apparent_1980s, 2) AS apparent_warming_c,
  ROUND(100 * w.pct_ge30_1980s,     1) AS pct_ge30_1980s,
  ROUND(100 * w.pct_ge30_2015_2024, 1) AS pct_ge30_2015_2024,
  ROUND(100 * w.pct_ge35_1980s,     1) AS pct_ge35_1980s,
  ROUND(100 * w.pct_ge35_2015_2024, 1) AS pct_ge35_2015_2024,
  COALESCE(mc.n_matches_2026, 0)        AS n_matches_2026,
  COALESCE(mc.n_group_matches, 0)       AS n_group_matches,
  COALESCE(mc.n_ko_matches, 0)          AS n_ko_matches,
  CURRENT_TIMESTAMP()                   AS reported_at
FROM wide w
LEFT JOIN match_counts mc USING (venue_id)
ORDER BY (w.apparent_2015_2024 - w.apparent_1980s) DESC
