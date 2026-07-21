/* @bruin
name: final_staging.shot_events
type: bq.sql
connection: bruin-playground-arsalan
description: |
  FIFA player-level shot events for Argentina and Spain in completed pre-final
  reports, including delivery type and an outcome group for analysis.

depends:
  - final_raw.fifa_match_facts

materialization:
  type: table
  strategy: create+replace

columns:
  - name: shot_id
    type: VARCHAR
    description: Stable natural key for one parsed shot event.
    primary_key: true
  - name: match_id
    type: VARCHAR
    description: FIFA match number.
  - name: team_name
    type: VARCHAR
    description: Argentina or Spain.
  - name: player_name
    type: VARCHAR
    description: Shooter name in the FIFA report.
  - name: event_minute
    type: INTEGER
    description: Shot minute in FIFA's report timeline.
  - name: outcome_group
    type: VARCHAR
    description: Goal, on target, off target, blocked, or incomplete.
  - name: delivery_type
    type: VARCHAR
    description: Delivery that preceded the shot, such as pass, corner, or cross.
  - name: outcome_detail
    type: VARCHAR
    description: FIFA outcome and body-part detail.
  - name: match_date
    type: DATE
    description: Local match date.

@bruin */

WITH deduped AS (
  SELECT *
  FROM `bruin-playground-arsalan.final_raw.fifa_match_facts`
  WHERE fact_type = 'shot'
    AND team_name IN ('Argentina', 'Spain')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY fact_id ORDER BY extracted_at DESC) = 1
)

SELECT
  TO_HEX(SHA256(CONCAT(match_id, '|', team_name, '|', entity_name, '|', CAST(event_minute AS STRING), '|', metric_name, '|', related_entity_name, '|', text_value))) AS shot_id,
  match_id,
  team_name,
  entity_name AS player_name,
  event_minute,
  metric_name AS outcome_group,
  related_entity_name AS delivery_type,
  text_value AS outcome_detail,
  match_date
FROM deduped
ORDER BY match_date, SAFE_CAST(match_id AS INT64), team_name, event_minute, player_name
