/* @bruin
name: final_reports.cumulative_xg_differential
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Cumulative expected-goals differential along each finalist's chronological
  seven-match pre-final path. This supports a descriptive path comparison,
  not an opponent-adjusted forecast.

depends:
  - final_reports.xg_trends

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
    description: Chronological pre-final match number for the team.
  - name: match_label
    type: VARCHAR
    description: Date, stage, and opponent label from the FIFA report.
  - name: match_xg_differential
    type: DOUBLE
    description: Team xG minus opponent xG in the individual match.
  - name: cumulative_xg_differential
    type: DOUBLE
    description: Running total of match xG differential through this match.

@bruin */

SELECT
  team_name,
  match_id,
  match_number,
  match_label,
  xg_differential AS match_xg_differential,
  SUM(xg_differential) OVER (
    PARTITION BY team_name
    ORDER BY match_number
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_xg_differential
FROM `bruin-playground-arsalan.final_reports.xg_trends`
ORDER BY team_name, match_number
