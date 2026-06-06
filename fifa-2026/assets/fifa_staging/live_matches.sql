/* @bruin
name: fifa_staging.live_matches
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Latest known match state for the FIFA World Cup 2026 live tracker. Raw match
  snapshots append on every extraction; this staging table keeps one latest row
  per match, normalizes status labels, parses local kickoff DATETIME, and joins
  stadium metadata.

depends:
  - fifa_raw.live_games
  - fifa_staging.live_stadiums

materialization:
  type: table
  strategy: create+replace

columns:
  - name: api_match_id
    type: VARCHAR
    description: worldcup26.ir match identifier.
    primary_key: true
    nullable: false
  - name: match_number
    type: INTEGER
    description: Numeric match number.
  - name: match_type
    type: VARCHAR
    description: Source match type, e.g. group, round16, quarter, semi, final.
  - name: stage_label
    type: VARCHAR
    description: Human-readable tournament stage label.
  - name: group_id
    type: VARCHAR
    description: Group letter for group-stage fixtures; null for knockout fixtures.
  - name: matchday
    type: INTEGER
    description: Source matchday number within the tournament phase.
  - name: kickoff_local
    type: TIMESTAMP
    description: Parsed scheduled kickoff timestamp in source local venue time.
  - name: match_date
    type: DATE
    description: Scheduled match date in source local venue time.
  - name: home_team_id
    type: VARCHAR
    description: worldcup26.ir home-team identifier.
  - name: away_team_id
    type: VARCHAR
    description: worldcup26.ir away-team identifier.
  - name: home_team_name
    type: VARCHAR
    description: Home team English display name.
  - name: away_team_name
    type: VARCHAR
    description: Away team English display name.
  - name: scoreline
    type: VARCHAR
    description: Scoreline string formatted as home-away goals.
  - name: home_score
    type: INTEGER
    description: Current home-team score in goals at extraction time.
  - name: away_score
    type: INTEGER
    description: Current away-team score in goals at extraction time.
  - name: total_goals
    type: INTEGER
    description: Sum of home and away goals at extraction time.
  - name: home_scorers
    type: VARCHAR
    description: Source-provided home scorer string, when available.
  - name: away_scorers
    type: VARCHAR
    description: Source-provided away scorer string, when available.
  - name: match_status
    type: VARCHAR
    description: "Normalized status: scheduled, live, or completed."
  - name: time_elapsed
    type: VARCHAR
    description: Source status/elapsed-time text.
  - name: stadium_id
    type: VARCHAR
    description: worldcup26.ir stadium identifier.
  - name: venue_city
    type: VARCHAR
    description: Host city English display name.
  - name: venue_country
    type: VARCHAR
    description: Host country English display name.
  - name: stadium_name
    type: VARCHAR
    description: Stadium English display name.
  - name: source_extracted_at
    type: TIMESTAMP
    description: UTC extraction timestamp of the retained source row.

@bruin */

WITH deduped AS (
  SELECT *
  FROM `bruin-playground-arsalan.fifa_raw.live_games`
  WHERE api_match_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY api_match_id ORDER BY extracted_at DESC) = 1
),
typed AS (
  SELECT
    api_match_id,
    SAFE_CAST(api_match_id AS INT64) AS match_number,
    match_type,
    CASE LOWER(match_type)
      WHEN 'group' THEN 'Group stage'
      WHEN 'r32' THEN 'Round of 32'
      WHEN 'round32' THEN 'Round of 32'
      WHEN 'round16' THEN 'Round of 16'
      WHEN 'r16' THEN 'Round of 16'
      WHEN 'quarter' THEN 'Quarter-final'
      WHEN 'qf' THEN 'Quarter-final'
      WHEN 'semi' THEN 'Semi-final'
      WHEN 'sf' THEN 'Semi-final'
      WHEN 'thirdplace' THEN 'Third-place match'
      WHEN 'third' THEN 'Third-place match'
      WHEN 'final' THEN 'Final'
      ELSE INITCAP(REPLACE(match_type, '_', ' '))
    END AS stage_label,
    group_id,
    matchday,
    TIMESTAMP(PARSE_DATETIME('%m/%d/%Y %H:%M', local_date)) AS kickoff_local,
    DATE(PARSE_DATETIME('%m/%d/%Y %H:%M', local_date)) AS match_date,
    home_team_id,
    away_team_id,
    home_team_name_en AS home_team_name,
    away_team_name_en AS away_team_name,
    home_score,
    away_score,
    CONCAT(CAST(home_score AS STRING), '-', CAST(away_score AS STRING)) AS scoreline,
    COALESCE(home_score, 0) + COALESCE(away_score, 0) AS total_goals,
    NULLIF(home_scorers, '') AS home_scorers,
    NULLIF(away_scorers, '') AS away_scorers,
    CASE
      WHEN finished THEN 'completed'
      WHEN LOWER(time_elapsed) IN ('notstarted', 'not_started', 'scheduled') THEN 'scheduled'
      ELSE 'live'
    END AS match_status,
    time_elapsed,
    stadium_id,
    extracted_at AS source_extracted_at
  FROM deduped
)
SELECT
  t.*,
  s.venue_city,
  s.venue_country,
  s.stadium_name
FROM typed t
LEFT JOIN `bruin-playground-arsalan.fifa_staging.live_stadiums` s USING (stadium_id)
ORDER BY match_number
