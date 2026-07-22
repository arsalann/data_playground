/* @bruin
name: final_staging.starters
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Official FIFA starting lineups for Argentina and Spain in completed reports,
  including the final. Each team-match must retain exactly 11 starters.

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
  - name: shirt_number
    type: INTEGER
    description: Official shirt number of the starter.
    primary_key: true
  - name: player_name
    type: VARCHAR
    description: Player name in the FIFA match report.
  - name: position
    type: VARCHAR
    description: FIFA position code in the starting lineup.
  - name: match_date
    type: DATE
    description: Local match date.

@bruin */

WITH deduped AS (
  SELECT *
  FROM `bruin-playground-arsalan.final_raw.fifa_match_facts`
  WHERE fact_type = 'starter'
    AND team_name IN ('Argentina', 'Spain')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY match_id, team_name, CAST(numeric_value AS INT64)
    ORDER BY extracted_at DESC, source_hash DESC
  ) = 1
)

SELECT
  match_id,
  team_name,
  CAST(numeric_value AS INT64) AS shirt_number,
  entity_name AS player_name,
  text_value AS position,
  match_date
FROM deduped
QUALIFY COUNT(*) OVER (PARTITION BY match_id, team_name) = 11
ORDER BY match_date, SAFE_CAST(match_id AS INT64), team_name, shirt_number
