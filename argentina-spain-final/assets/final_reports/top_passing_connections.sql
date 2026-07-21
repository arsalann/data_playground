/* @bruin
name: final_reports.top_passing_connections
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Repeated top-five FIFA passing connections, ranked by mean share of team
  passes across completed pre-final reports.

depends:
  - final_staging.passing_links

materialization:
  type: table
  strategy: create+replace

columns:
  - name: team_name
    type: VARCHAR
    description: Argentina or Spain.
    primary_key: true
  - name: source_player
    type: VARCHAR
    description: Player making passes.
    primary_key: true
  - name: destination_player
    type: VARCHAR
    description: Player receiving passes.
    primary_key: true
  - name: appearances_in_top_five
    type: INTEGER
    description: Completed reports in which the connection appeared in FIFA’s top five.
  - name: mean_team_pass_share_pct
    type: DOUBLE
    description: Mean connection share of team passes in those appearances.
  - name: max_team_pass_share_pct
    type: DOUBLE
    description: Highest reported connection share in one match.

@bruin */

SELECT
  team_name,
  source_player,
  destination_player,
  COUNT(*) AS appearances_in_top_five,
  AVG(team_pass_share_pct) AS mean_team_pass_share_pct,
  MAX(team_pass_share_pct) AS max_team_pass_share_pct
FROM `bruin-playground-arsalan.final_staging.passing_links`
GROUP BY team_name, source_player, destination_player
QUALIFY ROW_NUMBER() OVER (PARTITION BY team_name ORDER BY appearances_in_top_five DESC, mean_team_pass_share_pct DESC, source_player, destination_player) <= 10
ORDER BY team_name, appearances_in_top_five DESC, mean_team_pass_share_pct DESC
