/* @bruin
name: report.stage_outcomes
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Returns one winner row per completed stage with a transparent outcome-shape
  label for the dashboard's stage-results table.

depends:
  - staging.stage_results

materialization:
  type: table
  strategy: create+replace

columns:
  - name: stage_number
    type: INTEGER
    description: Official completed stage number.
    primary_key: true
  - name: stage_date
    type: DATE
    description: Scheduled local date for the completed stage.
  - name: stage_name
    type: VARCHAR
    description: Published route label for the stage.
  - name: stage_type
    type: VARCHAR
    description: Road-stage, team-time-trial, or individual-time-trial classification.
  - name: winner_name
    type: VARCHAR
    description: Published winning rider or team name.
  - name: winner_team_name
    type: VARCHAR
    description: Published team of the winner where available.
  - name: winner_gap_to_second_seconds
    type: INTEGER
    description: Published runner-up gap in seconds when supplied by PCS.
  - name: outcome_shape
    type: VARCHAR
    description: Transparent result-shape classification; candidate breakaway means a winner margin above 10 seconds.

@bruin */

WITH deduped AS (
    SELECT *
    FROM staging.stage_results
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY stage_number, stage_rank
        ORDER BY extracted_at DESC, source_snapshot_id DESC
    ) = 1
)

SELECT
    stage_number,
    stage_date,
    stage_name,
    stage_type,
    rider_name AS winner_name,
    team_name AS winner_team_name,
    winner_gap_to_second_seconds,
    outcome_shape
FROM deduped
WHERE stage_rank = 1
ORDER BY stage_number;
