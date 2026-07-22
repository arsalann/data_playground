/* @bruin
name: final_reports.xg_trends
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Match-by-match xG, xG conceded, and xG differential for Argentina and Spain
  across their eight completed FIFA reports, including the final.

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
  - name: match_id
    type: VARCHAR
    description: FIFA match number.
    primary_key: true
  - name: match_number
    type: INTEGER
    description: Sequential tournament match number for each team.
  - name: match_label
    type: VARCHAR
    description: Date, stage, and opponent label for chart tooltip.
  - name: xg
    type: DOUBLE
    description: Team expected goals.
  - name: xg_conceded
    type: DOUBLE
    description: Opponent expected goals.
  - name: xg_differential
    type: DOUBLE
    description: Team xG minus opponent xG.

@bruin */

WITH target_matches AS (
  SELECT *
  FROM `bruin-playground-arsalan.final_staging.team_match_metrics`
  WHERE team_name IN ('Argentina', 'Spain')
),
enriched AS (
  SELECT
    t.*,
    o.xg AS xg_conceded,
    ROW_NUMBER() OVER (PARTITION BY t.team_name ORDER BY t.match_date, SAFE_CAST(t.match_id AS INT64)) AS match_number
  FROM target_matches t
  LEFT JOIN `bruin-playground-arsalan.final_staging.team_match_metrics` o
    ON t.match_id = o.match_id AND t.opponent_name = o.team_name
)

SELECT
  team_name,
  match_id,
  match_number,
  CONCAT(FORMAT_DATE('%d %b', match_date), ' · ', stage, ' v ', opponent_name) AS match_label,
  xg,
  xg_conceded,
  xg - xg_conceded AS xg_differential
FROM enriched
ORDER BY team_name, match_number
