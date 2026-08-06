/* @bruin
name: chess_peterthiel.report_session_position_performance
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Tests whether public score changes over the course of long, same-format
  playing sessions. A session is a sequence of dominant-time-control games
  whose consecutive recorded end times are no more than 30 minutes apart.
  Only sessions with at least ten games are included. The result is
  observational and does not prove a fatigue effect.

depends:
  - chess_peterthiel.stage_games_enriched

materialization:
  type: table
  strategy: create+replace

columns:
  - name: session_position_bucket
    type: VARCHAR
    description: Readable range of a game's ordinal position within a qualifying public session.
    primary_key: true
    nullable: false
  - name: session_position_order
    type: INTEGER
    description: Natural sort order for the session-position bucket.
    nullable: false
  - name: games
    type: INTEGER
    description: Number of public games in the session-position bucket.
  - name: sessions
    type: INTEGER
    description: Number of 30-minute-gap-defined qualifying sessions contributing games to the bucket.
  - name: actual_score_pct
    type: DOUBLE
    description: Conventional public game score as a percentage of possible points.
  - name: expected_score_proxy_pct
    type: DOUBLE
    description: Rating-derived expected-score proxy in percent.
  - name: performance_residual_proxy_pp
    type: DOUBLE
    description: Actual score minus expected-score proxy in percentage points.
  - name: average_opponent_rating_after
    type: DOUBLE
    description: Mean opponent public post-game rating snapshot in the bucket.

@bruin */

WITH dominant_time_control AS (
    SELECT time_control
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched`
    GROUP BY time_control
    QUALIFY ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, time_control) = 1
),

ordered_games AS (
    SELECT
        games.*,
        LAG(game_end_time_utc) OVER (
            ORDER BY game_end_time_utc, game_uuid
        ) AS prior_game_end_time_utc
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched` AS games
    INNER JOIN dominant_time_control USING (time_control)
),

session_labeled AS (
    SELECT
        *,
        SUM(
            IF(
                prior_game_end_time_utc IS NULL
                    OR TIMESTAMP_DIFF(game_end_time_utc, prior_game_end_time_utc, MINUTE) > 30,
                1,
                0
            )
        ) OVER (
            ORDER BY game_end_time_utc, game_uuid
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_number
    FROM ordered_games
),

positioned_games AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY session_number
            ORDER BY game_end_time_utc, game_uuid
        ) AS session_game_number,
        COUNT(*) OVER (PARTITION BY session_number) AS session_games
    FROM session_labeled
),

bucketed_games AS (
    SELECT
        CASE
            WHEN session_game_number = 1 THEN 'Game 1'
            WHEN session_game_number BETWEEN 2 AND 5 THEN 'Games 2–5'
            WHEN session_game_number BETWEEN 6 AND 10 THEN 'Games 6–10'
            WHEN session_game_number BETWEEN 11 AND 20 THEN 'Games 11–20'
            ELSE 'Game 21+'
        END AS session_position_bucket,
        CASE
            WHEN session_game_number = 1 THEN 1
            WHEN session_game_number BETWEEN 2 AND 5 THEN 2
            WHEN session_game_number BETWEEN 6 AND 10 THEN 3
            WHEN session_game_number BETWEEN 11 AND 20 THEN 4
            ELSE 5
        END AS session_position_order,
        *
    FROM positioned_games
    WHERE session_games >= 10
)

SELECT
    session_position_bucket,
    session_position_order,
    COUNT(*) AS games,
    COUNT(DISTINCT session_number) AS sessions,
    100.0 * AVG(actual_score) AS actual_score_pct,
    100.0 * AVG(expected_score_proxy) AS expected_score_proxy_pct,
    100.0 * AVG(performance_residual_proxy) AS performance_residual_proxy_pp,
    AVG(opponent_rating_after) AS average_opponent_rating_after
FROM bucketed_games
GROUP BY session_position_bucket, session_position_order
ORDER BY session_position_order
