/* @bruin
name: fifa_reports.live_current_match_detail
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Dashboard-ready detail rows for live matches. If no match is live, falls back
  to the next scheduled fixture so the current-match tab remains useful before
  kickoff. DAC does not currently support row-click drilldowns, so this table
  acts as the selected-detail panel for the live scoreboard.

depends:
  - fifa_staging.live_matches

materialization:
  type: table
  strategy: create+replace

columns:
  - name: detail_order
    type: INTEGER
    description: Display order for detail rows.
    primary_key: true
  - name: detail_name
    type: VARCHAR
    description: Detail label.
  - name: detail_value
    type: VARCHAR
    description: Detail value.

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
)
SELECT 1 AS detail_order, 'Match' AS detail_name, CONCAT('Match ', CAST(match_number AS STRING)) AS detail_value FROM selected_match
UNION ALL
SELECT 2, 'Status', match_status FROM selected_match
UNION ALL
SELECT 3, 'Elapsed/source status', COALESCE(time_elapsed, 'Unavailable') FROM selected_match
UNION ALL
SELECT 4, 'Scoreline', CONCAT(home_team_name, ' ', scoreline, ' ', away_team_name) FROM selected_match
UNION ALL
SELECT 5, 'Stage/group', CONCAT(stage_label, COALESCE(CONCAT(' - Group ', group_id), '')) FROM selected_match
UNION ALL
SELECT 6, 'Kickoff local', FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', kickoff_local) FROM selected_match
UNION ALL
SELECT 7, 'Venue', CONCAT(COALESCE(venue_city, 'Unknown'), ' - ', COALESCE(stadium_name, 'Unknown stadium')) FROM selected_match
UNION ALL
SELECT 8, 'Home scorers', COALESCE(home_scorers, 'Not reported by source') FROM selected_match
UNION ALL
SELECT 9, 'Away scorers', COALESCE(away_scorers, 'Not reported by source') FROM selected_match
ORDER BY detail_order
