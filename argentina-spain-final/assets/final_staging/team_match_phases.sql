/* @bruin
name: final_staging.team_match_phases
type: bq.sql
connection: bruin-playground-arsalan
description: |
  FIFA in- and out-of-possession phase shares for Argentina and Spain only.
  Shares are retained as separate phase measures because FIFA's phase taxonomy
  can overlap; they must not be summed into a false 100% composition.

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
  - name: phase_group
    type: VARCHAR
    description: In-possession or out-of-possession phase family.
    primary_key: true
  - name: phase_name
    type: VARCHAR
    description: Human-readable FIFA phase label.
    primary_key: true
  - name: phase_share_pct
    type: DOUBLE
    description: FIFA phase share percentage, kept independently rather than summed.
  - name: match_date
    type: DATE
    description: Local match date.
  - name: stage
    type: VARCHAR
    description: Tournament stage.

@bruin */

WITH deduped AS (
  SELECT *
  FROM `bruin-playground-arsalan.final_raw.fifa_match_facts`
  WHERE fact_type = 'phase'
    AND team_name IN ('Argentina', 'Spain')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY match_id, team_name, metric_name
    ORDER BY extracted_at DESC, source_hash DESC
  ) = 1
)

SELECT
  match_id,
  team_name,
  text_value AS phase_group,
  INITCAP(REPLACE(metric_name, '_', ' ')) AS phase_name,
  numeric_value AS phase_share_pct,
  match_date,
  stage
FROM deduped
WHERE numeric_value BETWEEN 0 AND 100
ORDER BY match_date, SAFE_CAST(match_id AS INT64), team_name, phase_group, phase_name
