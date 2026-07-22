/* @bruin
name: final_staging.team_match_metrics
type: bq.sql
connection: bruin-playground-arsalan
description: |
  One row per team and completed FIFA report, including the final, with core
  match metrics deduplicated from the append-only parsed fact table.

depends:
  - final_raw.fifa_match_facts

materialization:
  type: table
  strategy: create+replace

columns:
  - name: match_id
    type: VARCHAR
    description: FIFA World Cup 2026 match number.
    primary_key: true
  - name: team_name
    type: VARCHAR
    description: Team represented by this row.
    primary_key: true
  - name: match_date
    type: DATE
    description: Local match date from FIFA.
  - name: stage
    type: VARCHAR
    description: Tournament stage.
  - name: opponent_name
    type: VARCHAR
    description: Opponent in the FIFA report.
  - name: goals
    type: DOUBLE
    description: Goals scored in the match.
  - name: xg
    type: DOUBLE
    description: Expected goals from the FIFA report.
  - name: possession_pct
    type: DOUBLE
    description: Possession percentage reported by FIFA.
  - name: attempts
    type: DOUBLE
    description: Attempts at goal.
  - name: attempts_on_target
    type: DOUBLE
    description: Attempts at goal on target.
  - name: total_passes
    type: DOUBLE
    description: Total passes attempted.
  - name: completed_passes
    type: DOUBLE
    description: Completed passes.
  - name: completed_line_breaks
    type: DOUBLE
    description: Completed line breaks.
  - name: ball_progressions
    type: DOUBLE
    description: Ball progressions.
  - name: source_extracted_at
    type: TIMESTAMP
    description: Latest retained raw-source extraction timestamp.

@bruin */

WITH deduped AS (
  SELECT *
  FROM `bruin-playground-arsalan.final_raw.fifa_match_facts`
  WHERE fact_type = 'team_metric'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY match_id, team_name, metric_name
    ORDER BY extracted_at DESC, source_hash DESC
  ) = 1
)

SELECT
  match_id,
  team_name,
  ANY_VALUE(match_date) AS match_date,
  ANY_VALUE(stage) AS stage,
  ANY_VALUE(opponent_name) AS opponent_name,
  MAX(IF(metric_name = 'goals', numeric_value, NULL)) AS goals,
  MAX(IF(metric_name = 'xg', numeric_value, NULL)) AS xg,
  MAX(IF(metric_name = 'possession_pct', numeric_value, NULL)) AS possession_pct,
  MAX(IF(metric_name = 'attempts', numeric_value, NULL)) AS attempts,
  MAX(IF(metric_name = 'attempts_on_target', numeric_value, NULL)) AS attempts_on_target,
  MAX(IF(metric_name = 'total_passes', numeric_value, NULL)) AS total_passes,
  MAX(IF(metric_name = 'completed_passes', numeric_value, NULL)) AS completed_passes,
  MAX(IF(metric_name = 'completed_line_breaks', numeric_value, NULL)) AS completed_line_breaks,
  MAX(IF(metric_name = 'ball_progressions', numeric_value, NULL)) AS ball_progressions,
  MAX(extracted_at) AS source_extracted_at
FROM deduped
GROUP BY match_id, team_name
HAVING goals IS NOT NULL
  AND xg IS NOT NULL
  AND possession_pct IS NOT NULL
  AND attempts IS NOT NULL
  AND attempts_on_target IS NOT NULL
  AND total_passes IS NOT NULL
  AND completed_passes IS NOT NULL
  AND completed_line_breaks IS NOT NULL
  AND ball_progressions IS NOT NULL
ORDER BY match_date, SAFE_CAST(match_id AS INT64), team_name
