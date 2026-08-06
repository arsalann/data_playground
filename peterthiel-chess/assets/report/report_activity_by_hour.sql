/* @bruin
name: chess_peterthiel.report_activity_by_hour
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Summarizes public game volume and score by UTC hour for the dominant Chess.com
  time control. It supports a habit chart without making claims about local time
  or offline behavior.

depends:
  - chess_peterthiel.stage_games_enriched

materialization:
  type: table
  strategy: create+replace

columns:
  - name: hour_utc
    type: INTEGER
    description: UTC hour-of-day from 0 through 23.
    primary_key: true
    nullable: false
  - name: hour_label_utc
    type: VARCHAR
    description: Readable UTC hour label formatted as HH:00.
  - name: games
    type: INTEGER
    description: Number of dominant-time-control public games ending in the UTC hour.
  - name: score_rate_pct
    type: DOUBLE
    description: Percentage of possible conventional score earned by games ending in the UTC hour.

@bruin */

WITH dominant_time_control AS (
    SELECT time_control
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched`
    GROUP BY time_control
    QUALIFY ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, time_control) = 1
)

SELECT
    game_hour_utc AS hour_utc,
    FORMAT('%02d:00', game_hour_utc) AS hour_label_utc,
    COUNT(*) AS games,
    100.0 * AVG(actual_score) AS score_rate_pct
FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched`
INNER JOIN dominant_time_control USING (time_control)
GROUP BY game_hour_utc
ORDER BY hour_utc
