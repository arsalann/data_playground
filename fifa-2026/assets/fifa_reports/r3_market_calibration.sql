/* @bruin

name: fifa_reports.r3_market_calibration
type: bq.sql
description: |
  R3 — Polymarket "Will X win 2026" calibration vs FIFA April-2026 ranking
  softmax. Two outputs combined into one wide row per team plus a vig field
  joined in.

  Adds ranking-bucket grouping so the dashboard can show systematic
  favorite-overpricing vs longshot-underpricing — the classic favorite-longshot
  bias inverted (in tournaments with a single winner, public bettors load up
  on the small set of "stars" and starve the field).
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - reports
  - r3

materialization:
  type: table
  strategy: create+replace

depends:
  - fifa_reports.h3_market_vs_ranking
  - fifa_staging.team_market_implied_prob

@bruin */

WITH base AS (
  SELECT
    h.fifa_code,
    h.team_name,
    h.confederation,
    h.fifa_rank,
    h.fifa_points,
    h.ranking_implied_prob,
    h.market_implied_prob,
    h.market_yes_price,
    h.gap_pp,
    h.flag,
    h.market_volume,
    h.is_stale
  FROM `bruin-playground-arsalan.fifa_reports.h3_market_vs_ranking` h
  WHERE h.market_implied_prob IS NOT NULL
),
vig AS (
  /* Sum of yes_prices across the 48 teams; vig_pct = (sum - 1) * 100. */
  SELECT
    ROUND(SUM(market_yes_price), 4)                AS sum_yes_price,
    ROUND((SUM(market_yes_price) - 1) * 100, 2)    AS vig_pct,
    COUNT(*)                                       AS n_teams_with_market
  FROM base
)
SELECT
  b.fifa_code,
  b.team_name,
  b.confederation,
  b.fifa_rank,
  b.fifa_points,
  ROUND(100 * b.ranking_implied_prob, 3) AS ranking_pct,
  ROUND(100 * b.market_implied_prob, 3)  AS market_pct,
  b.market_yes_price,
  b.gap_pp,
  b.flag,
  /* Bucket teams into rank quartiles for bias analysis. */
  NTILE(4) OVER (ORDER BY b.fifa_rank ASC) AS rank_quartile,
  CASE
    WHEN b.fifa_rank <= 12 THEN 'top_12_favorites'
    WHEN b.fifa_rank <= 24 THEN 'mid_12_24'
    WHEN b.fifa_rank <= 36 THEN 'mid_25_36'
    ELSE 'longshots_37_48'
  END                            AS rank_band,
  b.market_volume,
  b.is_stale,
  v.sum_yes_price,
  v.vig_pct,
  v.n_teams_with_market,
  CURRENT_TIMESTAMP()              AS reported_at
FROM base b, vig v
ORDER BY b.fifa_rank
