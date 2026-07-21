/* @bruin
name: final_staging.squad_start_counts
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Latest official FIFA squad lists enriched with each player's pre-final start
  count, joined by team and official squad number rather than fragile name text.

depends:
  - final_raw.official_squads
  - final_staging.starters

materialization:
  type: table
  strategy: create+replace

columns:
  - name: team_name
    type: VARCHAR
    description: Argentina or Spain.
    primary_key: true
  - name: squad_number
    type: INTEGER
    description: Official FIFA shirt number.
    primary_key: true
  - name: player_name
    type: VARCHAR
    description: Official FIFA squad-list player name.
  - name: position
    type: VARCHAR
    description: Official FIFA squad-list position code.
  - name: club
    type: VARCHAR
    description: Club listed by FIFA.
  - name: tournament_starts
    type: INTEGER
    description: Number of parsed starts across the seven pre-final reports.
  - name: coach
    type: VARCHAR
    description: Head coach listed in the latest FIFA squad PDF.
  - name: source_version
    type: VARCHAR
    description: Latest FIFA squad-document version.

@bruin */

WITH latest_squads AS (
  SELECT *
  FROM `bruin-playground-arsalan.final_raw.official_squads`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY team_name, squad_number ORDER BY extracted_at DESC, source_hash DESC) = 1
),
start_counts AS (
  SELECT team_name, shirt_number AS squad_number, COUNT(*) AS tournament_starts
  FROM `bruin-playground-arsalan.final_staging.starters`
  GROUP BY team_name, squad_number
)

SELECT
  s.team_name,
  s.squad_number,
  s.player_name,
  s.position,
  s.club,
  COALESCE(c.tournament_starts, 0) AS tournament_starts,
  s.coach,
  s.source_version
FROM latest_squads s
LEFT JOIN start_counts c USING (team_name, squad_number)
ORDER BY team_name, squad_number
