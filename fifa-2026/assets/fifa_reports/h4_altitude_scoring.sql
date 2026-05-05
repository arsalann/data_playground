/* @bruin

name: fifa_reports.h4_altitude_scoring
type: bq.sql
description: |
  H4 — Altitude effect on scoring. Aggregates historical World Cup matches
  (2010-2022) into altitude bands and reports goals-per-match with a normal-
  approximation 95% confidence interval.

  Altitude bands:
    - Low      : <= 500 m
    - Moderate : 500 - 1500 m
    - High     : 1500 - 2200 m
    - Extreme  : > 2200 m

  Mexico City Azteca (2240 m) and Guadalajara Akron (1566 m) are the two
  FIFA-2026 venues that fall above 1500 m. The High and Extreme bands are
  the ones the H4 narrative leans on; sample sizes there are small (footnote).

  Caveats: era confounds (rule changes, tactical evolution); Liga MX is
  excluded from this build (licensing) so all rows are international WC.
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - reports
  - h4

materialization:
  type: table
  strategy: create+replace

depends:
  - fifa_staging.altitude_match_panel

@bruin */

WITH banded AS (
  SELECT
    altitude_band,
    era_bucket,
    source,
    goals_total,
    home_goals,
    away_goals
  FROM `bruin-playground-arsalan.fifa_staging.altitude_match_panel`
),
agg AS (
  SELECT
    altitude_band,
    era_bucket,
    source,
    COUNT(*)                                              AS n_matches,
    AVG(goals_total)                                      AS goals_per_match,
    STDDEV_SAMP(goals_total)                              AS sd_goals,
    AVG(home_goals)                                       AS home_goals_per_match,
    AVG(away_goals)                                       AS away_goals_per_match
  FROM banded
  GROUP BY altitude_band, era_bucket, source
)
SELECT
  altitude_band,
  era_bucket,
  source,
  n_matches,
  ROUND(goals_per_match,      3) AS goals_per_match,
  ROUND(home_goals_per_match, 3) AS home_goals_per_match,
  ROUND(away_goals_per_match, 3) AS away_goals_per_match,
  /* 95% normal-approx CI on the mean: gpm ± 1.96 * sd / sqrt(n) */
  ROUND(goals_per_match - 1.96 * sd_goals / SQRT(NULLIF(n_matches, 0)), 3) AS ci95_low,
  ROUND(goals_per_match + 1.96 * sd_goals / SQRT(NULLIF(n_matches, 0)), 3) AS ci95_high,
  CASE
    WHEN altitude_band IN ('Low', 'Moderate') THEN 'reference'
    WHEN altitude_band IN ('High', 'Extreme') THEN 'fifa_2026_relevant'
  END                            AS band_role,
  CURRENT_TIMESTAMP() AS reported_at
FROM agg
ORDER BY
  CASE altitude_band
    WHEN 'Low'      THEN 1
    WHEN 'Moderate' THEN 2
    WHEN 'High'     THEN 3
    WHEN 'Extreme'  THEN 4
  END,
  era_bucket
