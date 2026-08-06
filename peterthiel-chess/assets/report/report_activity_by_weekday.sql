/* @bruin
name: chess_peterthiel.report_activity_by_weekday
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Summarizes public game volume and score by UTC weekday for the dominant
  Chess.com time control. UTC is explicit because a public account record does
  not establish the player's local time zone.

depends:
  - chess_peterthiel.stage_games_enriched

materialization:
  type: table
  strategy: create+replace

columns:
  - name: day_of_week_utc
    type: INTEGER
    description: UTC weekday ordinal where 1 is Sunday and 7 is Saturday.
    primary_key: true
    nullable: false
  - name: day_name_utc
    type: VARCHAR
    description: English UTC weekday label.
  - name: games
    type: INTEGER
    description: Number of dominant-time-control public games ending on the UTC weekday.
  - name: score_rate_pct
    type: DOUBLE
    description: Percentage of possible conventional score earned on the UTC weekday.

@bruin */

WITH dominant_time_control AS (
    SELECT time_control
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched`
    GROUP BY time_control
    QUALIFY ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, time_control) = 1
)

SELECT
    day_of_week_utc,
    ANY_VALUE(day_name_utc) AS day_name_utc,
    COUNT(*) AS games,
    100.0 * AVG(actual_score) AS score_rate_pct
FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched`
INNER JOIN dominant_time_control USING (time_control)
GROUP BY day_of_week_utc
ORDER BY day_of_week_utc
