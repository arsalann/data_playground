/* @bruin
name: fifa_staging.live_teams
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Latest team reference rows from the worldcup26.ir live tracker source.
  Deduplicates by team identifier and keeps the newest extracted_at snapshot.

depends:
  - fifa_raw.live_teams

materialization:
  type: table
  strategy: create+replace

columns:
  - name: team_id
    type: VARCHAR
    description: worldcup26.ir team identifier.
    primary_key: true
    nullable: false
  - name: team_name
    type: VARCHAR
    description: Team English display name.
  - name: fifa_code
    type: VARCHAR
    description: FIFA three-letter team code.
  - name: group_id
    type: VARCHAR
    description: Group letter A-L.
  - name: flag_url
    type: VARCHAR
    description: Source flag image URL.
  - name: iso2
    type: VARCHAR
    description: Source country/territory code.
  - name: source_extracted_at
    type: TIMESTAMP
    description: UTC extraction timestamp of the retained source row.

@bruin */

WITH deduped AS (
  SELECT *
  FROM `bruin-playground-arsalan.fifa_raw.live_teams`
  WHERE team_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY extracted_at DESC) = 1
)
SELECT
  team_id,
  team_name_en AS team_name,
  fifa_code,
  group_id,
  flag_url,
  iso2,
  extracted_at AS source_extracted_at
FROM deduped
ORDER BY group_id, team_name
