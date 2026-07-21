/* @bruin
name: final_reports.shot_outcomes
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Argentina and Spain shot-event outcomes grouped by the FIFA delivery type.

depends:
  - final_staging.shot_events

materialization:
  type: table
  strategy: create+replace

columns:
  - name: team_name
    type: VARCHAR
    description: Argentina or Spain.
    primary_key: true
  - name: delivery_type
    type: VARCHAR
    description: FIFA delivery type preceding the shot.
    primary_key: true
  - name: outcome_group
    type: VARCHAR
    description: Goal, on target, off target, blocked, or incomplete.
    primary_key: true
  - name: shot_count
    type: INTEGER
    description: Number of parsed shot events.

@bruin */

SELECT
  team_name,
  delivery_type,
  outcome_group,
  COUNT(*) AS shot_count
FROM `bruin-playground-arsalan.final_staging.shot_events`
GROUP BY team_name, delivery_type, outcome_group
ORDER BY team_name, delivery_type, outcome_group
