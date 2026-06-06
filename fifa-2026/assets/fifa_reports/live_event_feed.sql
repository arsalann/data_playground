/* @bruin
name: fifa_reports.live_event_feed
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Source-limited event feed for the current live-match tab. worldcup26.ir does
  not expose minute-by-minute events, player touches, substitutions, cards,
  shots, possession, or xG, so this table surfaces scorer strings when present
  and otherwise emits an explicit limitation row.

depends:
  - fifa_staging.live_matches

materialization:
  type: table
  strategy: create+replace

columns:
  - name: event_order
    type: INTEGER
    description: Display order for event rows.
    primary_key: true
  - name: minute
    type: VARCHAR
    description: Match minute or source status when exact minute is unavailable.
  - name: team
    type: VARCHAR
    description: Team associated with the event row.
  - name: event_type
    type: VARCHAR
    description: Event category.
  - name: event_detail
    type: VARCHAR
    description: Source-provided or analyst-created event detail.

@bruin */

WITH selected_match AS (
  SELECT *
  FROM `bruin-playground-arsalan.fifa_staging.live_matches`
  QUALIFY ROW_NUMBER() OVER (
    ORDER BY
      CASE match_status
        WHEN 'live' THEN 1
        WHEN 'scheduled' THEN 2
        ELSE 3
      END,
      match_date,
      match_number
  ) = 1
),
scorer_rows AS (
  SELECT
    1 AS event_order,
    COALESCE(time_elapsed, 'No minute') AS minute,
    home_team_name AS team,
    'Scorer string' AS event_type,
    home_scorers AS event_detail
  FROM selected_match
  WHERE home_scorers IS NOT NULL

  UNION ALL

  SELECT
    2 AS event_order,
    COALESCE(time_elapsed, 'No minute') AS minute,
    away_team_name AS team,
    'Scorer string' AS event_type,
    away_scorers AS event_detail
  FROM selected_match
  WHERE away_scorers IS NOT NULL
)
SELECT * FROM scorer_rows
UNION ALL
SELECT
  99 AS event_order,
  COALESCE(time_elapsed, 'pre-match') AS minute,
  'Both teams' AS team,
  'Source limitation' AS event_type,
  'Minute-by-minute player/team events are not exposed by the free worldcup26.ir endpoint. Add API-Football or another licensed live-stats source to populate this feed.' AS event_detail
FROM selected_match
WHERE NOT EXISTS (SELECT 1 FROM scorer_rows)
ORDER BY event_order
