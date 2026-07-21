/* @bruin
name: final_staging.player_line_breaks
type: bq.sql
connection: bruin-playground-arsalan
description: |
  FIFA player-level attempted and completed line breaks for Argentina and Spain
  in completed pre-final matches.

depends:
  - final_raw.fifa_match_facts

materialization:
  type: table
  strategy: create+replace

columns:
  - name: match_id
    type: VARCHAR
    description: FIFA match number.
    primary_key: true
  - name: team_name
    type: VARCHAR
    description: Argentina or Spain.
    primary_key: true
  - name: player_name
    type: VARCHAR
    description: Player in the FIFA line-break table.
    primary_key: true
  - name: attempted_line_breaks
    type: DOUBLE
    description: Player attempted line breaks.
  - name: completed_line_breaks
    type: DOUBLE
    description: Player completed line breaks.
  - name: line_break_completion_pct
    type: DOUBLE
    description: Player line-break completion rate in percent.
  - name: match_date
    type: DATE
    description: Local match date.

@bruin */

WITH deduped AS (
  SELECT *
  FROM `bruin-playground-arsalan.final_raw.fifa_match_facts`
  WHERE fact_type = 'player_line_break'
    AND team_name IN ('Argentina', 'Spain')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY match_id, team_name, entity_name, metric_name
    ORDER BY extracted_at DESC, source_hash DESC
  ) = 1
)

SELECT
  match_id,
  team_name,
  entity_name AS player_name,
  MAX(IF(metric_name = 'attempted_line_breaks', numeric_value, NULL)) AS attempted_line_breaks,
  MAX(IF(metric_name = 'completed_line_breaks', numeric_value, NULL)) AS completed_line_breaks,
  MAX(IF(metric_name = 'line_break_completion_pct', numeric_value, NULL)) AS line_break_completion_pct,
  ANY_VALUE(match_date) AS match_date
FROM deduped
GROUP BY match_id, team_name, player_name
ORDER BY match_date, SAFE_CAST(match_id AS INT64), team_name, player_name
