/* @bruin
name: final_reports.goals_xg_match
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Goals, xG, and xG conceded for each Argentina and Spain pre-final match.

depends:
  - final_reports.xg_trends
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
    description: Sequential pre-final match number.
  - name: match_label
    type: VARCHAR
    description: Date, stage, and opponent label.
  - name: goals
    type: DOUBLE
    description: Goals scored.
  - name: xg
    type: DOUBLE
    description: Expected goals.
  - name: xg_conceded
    type: DOUBLE
    description: Opponent expected goals.

@bruin */

SELECT
  x.team_name,
  x.match_id,
  x.match_number,
  x.match_label,
  m.goals,
  x.xg,
  x.xg_conceded
FROM `bruin-playground-arsalan.final_reports.xg_trends` x
JOIN `bruin-playground-arsalan.final_staging.team_match_metrics` m
  USING (team_name, match_id)
ORDER BY team_name, match_number
