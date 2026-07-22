/* @bruin
name: final_reports.team_kpis
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Tournament KPIs for Argentina and Spain, calculated from all eight completed
  FIFA reports per team, including the final.

depends:
  - final_staging.team_match_metrics

materialization:
  type: table
  strategy: create+replace

columns:
  - name: team_name
    type: VARCHAR
    description: Argentina or Spain.
    primary_key: true
  - name: completed_matches
    type: INTEGER
    description: Number of completed FIFA reports included.
  - name: goals_per_match
    type: DOUBLE
    description: Average goals scored per completed match.
  - name: xg_per_match
    type: DOUBLE
    description: Average expected goals per completed match.
  - name: xg_conceded_per_match
    type: DOUBLE
    description: Average opponent expected goals per completed match.
  - name: xg_differential_per_match
    type: DOUBLE
    description: Average xG minus opponent xG per completed match.
  - name: possession_pct
    type: DOUBLE
    description: Average FIFA possession percentage.
  - name: source_as_of
    type: TIMESTAMP
    description: Latest source extraction timestamp in the KPI input.

@bruin */

WITH target_matches AS (
  SELECT *
  FROM `bruin-playground-arsalan.final_staging.team_match_metrics`
  WHERE team_name IN ('Argentina', 'Spain')
),
with_opponents AS (
  SELECT
    t.*,
    o.xg AS xg_conceded
  FROM target_matches t
  LEFT JOIN `bruin-playground-arsalan.final_staging.team_match_metrics` o
    ON t.match_id = o.match_id
   AND t.opponent_name = o.team_name
)

SELECT
  team_name,
  COUNT(*) AS completed_matches,
  AVG(goals) AS goals_per_match,
  AVG(xg) AS xg_per_match,
  AVG(xg_conceded) AS xg_conceded_per_match,
  AVG(xg - xg_conceded) AS xg_differential_per_match,
  AVG(possession_pct) AS possession_pct,
  MAX(source_extracted_at) AS source_as_of
FROM with_opponents
GROUP BY team_name
HAVING completed_matches = 8
ORDER BY team_name
