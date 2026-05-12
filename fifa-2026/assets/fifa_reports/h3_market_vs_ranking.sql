/* @bruin

name: fifa_reports.h3_market_vs_ranking
type: bq.sql
description: |
  H3 — Polymarket implied odds vs. April-2026 FIFA-ranking implied odds. One
  row per qualified team. gap_pp = (market_implied_prob - ranking_implied_prob)
  in percentage points; positive means the market prices the team above what
  FIFA points alone would imply.

  flag = 'over_priced'  when gap_pp > +5 pp,
         'under_priced' when gap_pp < -5 pp,
         'aligned'      otherwise.

  Stale-price guard: tokens with no CLOB tick in the last 7 days carry
  is_stale = TRUE; H3 dashboard greys these rows out and surfaces
  last_trade_at + market_volume so the reader can judge liquidity.
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - reports
  - h3

materialization:
  type: table
  strategy: create+replace

depends:
  - fifa_staging.team_market_implied_prob
  - fifa_staging.team_ranking_april2026
  - fifa_raw.qualified_teams

@bruin */

SELECT
  t.fifa_code,
  t.name                              AS team_name,
  t.confederation,
  r.fifa_rank,
  r.fifa_points,
  r.ranking_implied_prob,
  m.market_implied_prob,
  m.yes_price                         AS market_yes_price,
  ROUND(100 * (m.market_implied_prob - r.ranking_implied_prob), 2) AS gap_pp,
  CASE
    WHEN m.market_implied_prob IS NULL THEN 'no_market'
    WHEN (m.market_implied_prob - r.ranking_implied_prob) >  0.05 THEN 'over_priced'
    WHEN (m.market_implied_prob - r.ranking_implied_prob) < -0.05 THEN 'under_priced'
    ELSE 'aligned'
  END                                 AS flag,
  m.market_volume,
  m.last_trade_at,
  m.hours_since_last_trade,
  m.is_stale,
  CURRENT_TIMESTAMP() AS reported_at
FROM `bruin-playground-arsalan.fifa_raw.qualified_teams`              t
LEFT JOIN `bruin-playground-arsalan.fifa_staging.team_ranking_april2026`   r USING (fifa_code)
LEFT JOIN `bruin-playground-arsalan.fifa_staging.team_market_implied_prob` m USING (fifa_code)
ORDER BY ABS(COALESCE(m.market_implied_prob, 0) - COALESCE(r.ranking_implied_prob, 0)) DESC
