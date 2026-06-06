/* @bruin
name: fifa_reports.live_scoreboard
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Dashboard-ready scoreboard from the latest match state. Live matches sort
  first, then upcoming scheduled fixtures, then completed fixtures.

depends:
  - fifa_staging.live_matches

materialization:
  type: table
  strategy: create+replace

columns:
  - name: sort_bucket
    type: INTEGER
    description: Sort helper putting live, scheduled, then completed matches in order.
  - name: match_number
    type: INTEGER
    description: Numeric match number.
  - name: match_date
    type: DATE
    description: Scheduled match date in source local venue time.
  - name: stage_label
    type: VARCHAR
    description: Human-readable tournament stage label.
  - name: group_id
    type: VARCHAR
    description: Group letter for group-stage fixtures; null for knockout fixtures.
  - name: matchup
    type: VARCHAR
    description: Home and away team display names.
  - name: scoreline
    type: VARCHAR
    description: Scoreline string formatted as home-away goals.
  - name: match_status
    type: VARCHAR
    description: "Normalized status: scheduled, live, or completed."
  - name: time_elapsed
    type: VARCHAR
    description: Source status/elapsed-time text.
  - name: venue_label
    type: VARCHAR
    description: Host city and stadium label.
  - name: source_extracted_at
    type: TIMESTAMP
    description: UTC extraction timestamp of the retained source row.

@bruin */

SELECT
  CASE match_status
    WHEN 'live' THEN 1
    WHEN 'scheduled' THEN 2
    ELSE 3
  END AS sort_bucket,
  match_number,
  match_date,
  stage_label,
  group_id,
  CONCAT(home_team_name, ' vs ', away_team_name) AS matchup,
  scoreline,
  match_status,
  time_elapsed,
  CONCAT(COALESCE(venue_city, 'Unknown'), ' - ', COALESCE(stadium_name, 'Unknown stadium')) AS venue_label,
  source_extracted_at
FROM `bruin-playground-arsalan.fifa_staging.live_matches`
ORDER BY sort_bucket, match_date, match_number
