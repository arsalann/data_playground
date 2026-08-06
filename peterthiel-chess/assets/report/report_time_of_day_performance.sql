/* @bruin
name: chess_peterthiel.report_time_of_day_performance
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Compares score with the documented rating-derived expected-score proxy for
  every UTC hour in the public account's dominant time control. It is a
  descriptive schedule association, not an inference about local time,
  fatigue, or causation.

depends:
  - chess_peterthiel.stage_games_enriched

materialization:
  type: table
  strategy: create+replace

columns:
  - name: game_hour_utc
    type: INTEGER
    description: UTC hour in which the public game ended, from 0 through 23.
    primary_key: true
    nullable: false
  - name: hour_label_utc
    type: VARCHAR
    description: Readable non-ISO UTC hour label used on the dashboard x-axis.
    nullable: false
  - name: games
    type: INTEGER
    description: Number of dominant-time-control public games ending in the UTC hour.
  - name: actual_score_pct
    type: DOUBLE
    description: Conventional public game score as a percentage of possible points.
  - name: expected_score_proxy_pct
    type: DOUBLE
    description: Rating-derived expected-score proxy in percent using a reconstructed player pre-game rating and opponent post-game rating.
  - name: performance_residual_proxy_pp
    type: DOUBLE
    description: Actual score minus the expected-score proxy in percentage points.
  - name: average_opponent_rating_after
    type: DOUBLE
    description: Mean opponent public post-game rating snapshot in the UTC hour.

@bruin */

WITH dominant_time_control AS (
    SELECT time_control
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched`
    GROUP BY time_control
    QUALIFY ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, time_control) = 1
)

SELECT
    game_hour_utc,
    FORMAT('%02d:00', game_hour_utc) AS hour_label_utc,
    COUNT(*) AS games,
    100.0 * AVG(actual_score) AS actual_score_pct,
    100.0 * AVG(expected_score_proxy) AS expected_score_proxy_pct,
    100.0 * AVG(performance_residual_proxy) AS performance_residual_proxy_pp,
    AVG(opponent_rating_after) AS average_opponent_rating_after
FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched`
INNER JOIN dominant_time_control USING (time_control)
GROUP BY game_hour_utc
ORDER BY game_hour_utc
