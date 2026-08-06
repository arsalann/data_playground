/* @bruin
name: chess_peterthiel.stage_player_sessions
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Groups consecutive public Chess.com games into playing sessions. A new session
  begins after a gap of more than 30 minutes between recorded game-end times.
  This operational definition supports reproducible counts, duration, and gap
  comparisons without inferring motives or off-platform activity.

depends:
  - chess_peterthiel.stage_games_enriched

materialization:
  type: table
  strategy: create+replace

columns:
  - name: session_id
    type: VARCHAR
    description: Stable sequential identifier for a 30-minute-gap-defined playing session.
    primary_key: true
    nullable: false
  - name: session_number
    type: INTEGER
    description: Chronological ordinal of the session across the available public archive.
  - name: session_start_time_utc
    type: TIMESTAMP
    description: UTC end time of the first recorded game in the session.
  - name: session_end_time_utc
    type: TIMESTAMP
    description: UTC end time of the final recorded game in the session.
  - name: session_date_utc
    type: DATE
    description: UTC date of the session's first recorded game.
  - name: session_duration_seconds
    type: INTEGER
    description: Elapsed seconds from first to final recorded game end time in the session.
  - name: games
    type: INTEGER
    description: Number of public games in the session.
  - name: wins
    type: INTEGER
    description: Number of target-player wins in the session.
  - name: draws
    type: INTEGER
    description: Number of target-player draws in the session.
  - name: losses
    type: INTEGER
    description: Number of target-player losses in the session.
  - name: score
    type: DOUBLE
    description: Sum of conventional target-player game scores within the session.
  - name: score_rate_pct
    type: DOUBLE
    description: Percentage of possible game score earned within the session.
  - name: gap_before_seconds
    type: INTEGER
    description: Exact elapsed seconds since the immediately preceding recorded game; null for the first archive game.
  - name: prior_game_end_time_utc
    type: TIMESTAMP
    description: UTC end time of the game immediately preceding the session.

@bruin */

WITH ordered_games AS (
    SELECT
        game_url,
        game_end_time_utc,
        game_date_utc,
        outcome,
        actual_score,
        LAG(game_end_time_utc) OVER (
            ORDER BY game_end_time_utc, game_uuid
        ) AS prior_game_end_time_utc
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched`
),

session_boundaries AS (
    SELECT
        *,
        CASE
            WHEN prior_game_end_time_utc IS NULL THEN 1
            WHEN TIMESTAMP_DIFF(game_end_time_utc, prior_game_end_time_utc, SECOND) > 1800 THEN 1
            ELSE 0
        END AS is_new_session
    FROM ordered_games
),

session_labeled AS (
    SELECT
        *,
        SUM(is_new_session) OVER (
            ORDER BY game_end_time_utc, game_url
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_number
    FROM session_boundaries
),

session_first_games AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY session_number
            ORDER BY game_end_time_utc, game_url
        ) AS session_game_order
    FROM session_labeled
)

SELECT
    CONCAT('session_', CAST(session_number AS STRING)) AS session_id,
    session_number,
    MIN(game_end_time_utc) AS session_start_time_utc,
    MAX(game_end_time_utc) AS session_end_time_utc,
    MIN(game_date_utc) AS session_date_utc,
    TIMESTAMP_DIFF(MAX(game_end_time_utc), MIN(game_end_time_utc), SECOND) AS session_duration_seconds,
    COUNT(*) AS games,
    COUNTIF(outcome = 'win') AS wins,
    COUNTIF(outcome = 'draw') AS draws,
    COUNTIF(outcome = 'loss') AS losses,
    SUM(actual_score) AS score,
    100.0 * SAFE_DIVIDE(SUM(actual_score), COUNT(*)) AS score_rate_pct,
    MAX(IF(session_game_order = 1, TIMESTAMP_DIFF(game_end_time_utc, prior_game_end_time_utc, SECOND), NULL)) AS gap_before_seconds,
    MAX(IF(session_game_order = 1, prior_game_end_time_utc, NULL)) AS prior_game_end_time_utc
FROM session_first_games
GROUP BY session_number
ORDER BY session_number
