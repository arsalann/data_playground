/* @bruin
name: final_reports.official_squads
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Presentation-ready latest official FIFA squads with current tournament start
  counts. This is a roster, not a final-starting-XI prediction.

depends:
  - final_staging.squad_start_counts

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
    description: Official shirt number.
    primary_key: true
  - name: player_name
    type: VARCHAR
    description: FIFA player name.
  - name: position
    type: VARCHAR
    description: FIFA position code.
  - name: club
    type: VARCHAR
    description: FIFA-listed club.
  - name: tournament_starts
    type: INTEGER
    description: Starts across parsed pre-final FIFA reports.
  - name: coach
    type: VARCHAR
    description: FIFA-listed head coach.
  - name: source_version
    type: VARCHAR
    description: FIFA squad-document version.

@bruin */

SELECT *
FROM `bruin-playground-arsalan.final_staging.squad_start_counts`
ORDER BY team_name, squad_number
