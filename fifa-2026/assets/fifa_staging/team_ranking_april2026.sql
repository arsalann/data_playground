/* @bruin

name: fifa_staging.team_ranking_april2026
type: bq.sql
description: |
  Cleaned April-2026 FIFA Men's Ranking with an Elo-style ranking-implied
  probability per qualified team. The probability is a softmax of FIFA points
  with temperature parameter τ = 200, then normalised across the 48 qualified
  teams.

  This is what H3 compares against the Polymarket implied probabilities — gaps
  flag teams the market prices above (or below) what their FIFA-points alone
  would suggest.
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - staging
  - ranking

materialization:
  type: table
  strategy: create+replace

depends:
  - fifa_raw.fifa_world_ranking
  - fifa_raw.qualified_teams

@bruin */

WITH r AS (
  SELECT
    fifa_code,
    rank,
    points,
    snapshot_date,
    source
  FROM `bruin-playground-arsalan.fifa_raw.fifa_world_ranking`
),
qualified_only AS (
  SELECT r.*
  FROM r
  JOIN `bruin-playground-arsalan.fifa_raw.qualified_teams` t USING (fifa_code)
),
softmax AS (
  SELECT
    fifa_code,
    rank,
    points,
    snapshot_date,
    source,
    EXP(points / 200.0) AS exp_score
  FROM qualified_only
)
SELECT
  fifa_code,
  rank                                            AS fifa_rank,
  points                                          AS fifa_points,
  snapshot_date,
  source                                          AS ranking_source,
  exp_score / SUM(exp_score) OVER ()              AS ranking_implied_prob,
  CURRENT_TIMESTAMP() AS staged_at
FROM softmax
