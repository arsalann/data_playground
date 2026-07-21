/* @bruin
name: final_staging.passing_links
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Top five player-to-player passing connections in each target-team FIFA report.
  FIFA publishes each link as a share of team passes, not a complete network.

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
  - name: source_player
    type: VARCHAR
    description: Player making the passes.
    primary_key: true
  - name: destination_player
    type: VARCHAR
    description: Player receiving the passes.
    primary_key: true
  - name: team_pass_share_pct
    type: DOUBLE
    description: Share of the team’s passes represented by the connection.
  - name: match_date
    type: DATE
    description: Local match date.

@bruin */

SELECT
  match_id,
  team_name,
  entity_name AS source_player,
  related_entity_name AS destination_player,
  numeric_value AS team_pass_share_pct,
  match_date
FROM `bruin-playground-arsalan.final_raw.fifa_match_facts`
WHERE fact_type = 'passing_connection'
  AND team_name IN ('Argentina', 'Spain')
QUALIFY ROW_NUMBER() OVER (PARTITION BY match_id, team_name, entity_name, related_entity_name ORDER BY extracted_at DESC, source_hash DESC) = 1
ORDER BY match_date, SAFE_CAST(match_id AS INT64), team_name, team_pass_share_pct DESC
