/* @bruin
name: fifa_reports.live_group_table
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Dashboard-ready group standings table with team names, FIFA codes, rank,
  record, goals, and points from the latest worldcup26.ir standings snapshot.

depends:
  - fifa_staging.live_group_standings

materialization:
  type: table
  strategy: create+replace

columns:
  - name: group_id
    type: VARCHAR
    description: Group letter A-L.
    primary_key: true
  - name: group_rank
    type: INTEGER
    description: Computed rank within group.
    primary_key: true
  - name: team_name
    type: VARCHAR
    description: Team English display name.
  - name: fifa_code
    type: VARCHAR
    description: FIFA three-letter team code.
  - name: record
    type: VARCHAR
    description: Wins-draws-losses record.
  - name: matches_played
    type: INTEGER
    description: Matches played.
  - name: points
    type: INTEGER
    description: Group-stage points.
  - name: goals_for
    type: INTEGER
    description: Goals scored.
  - name: goals_against
    type: INTEGER
    description: Goals conceded.
  - name: goal_difference
    type: INTEGER
    description: Goals for minus goals against.
  - name: source_extracted_at
    type: TIMESTAMP
    description: UTC extraction timestamp of the retained source row.

@bruin */

SELECT
  group_id,
  group_rank,
  team_name,
  fifa_code,
  CONCAT(CAST(wins AS STRING), '-', CAST(draws AS STRING), '-', CAST(losses AS STRING)) AS record,
  matches_played,
  points,
  goals_for,
  goals_against,
  goal_difference,
  source_extracted_at
FROM `bruin-playground-arsalan.fifa_staging.live_group_standings`
ORDER BY group_id, group_rank
