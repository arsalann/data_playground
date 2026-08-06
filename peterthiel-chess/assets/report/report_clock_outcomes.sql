/* @bruin
name: chess_peterthiel.report_clock_outcomes
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Relates public game outcomes to the minimum remaining clock recorded in the
  target player's PGN annotations for the dominant time control. The result is
  an association, not evidence that clock pressure caused a result.

depends:
  - chess_peterthiel.stage_games_enriched
  - chess_peterthiel.stage_game_move_features

materialization:
  type: table
  strategy: create+replace

columns:
  - name: min_clock_bucket
    type: VARCHAR
    description: Human-readable minimum remaining-clock bucket from public PGN annotations.
    primary_key: true
    nullable: false
  - name: min_clock_bucket_order
    type: INTEGER
    description: Ordered position from lowest to highest remaining-clock bucket.
  - name: games
    type: INTEGER
    description: Number of dominant-time-control public games in the minimum-clock bucket.
  - name: score_rate_pct
    type: DOUBLE
    description: Percentage of possible conventional score earned in the minimum-clock bucket.
  - name: win_rate_pct
    type: DOUBLE
    description: Percentage of games won in the minimum-clock bucket.
  - name: timeout_or_abandon_pct
    type: DOUBLE
    description: Percentage of games ending in timeout or abandonment in the minimum-clock bucket.
  - name: average_under_10_seconds_move_pct
    type: DOUBLE
    description: Mean percentage of target-player moves under ten seconds remaining within the game bucket.

@bruin */

WITH dominant_time_control AS (
    SELECT time_control
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched`
    GROUP BY time_control
    QUALIFY ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, time_control) = 1
),

clock_labeled AS (
    SELECT
        games.*,
        features.minimum_clock_seconds_remaining,
        features.clock_pressure_move_rate_pct,
        CASE
            WHEN features.minimum_clock_seconds_remaining IS NULL THEN 'No public clock'
            WHEN features.minimum_clock_seconds_remaining < 5 THEN 'Under 5 seconds'
            WHEN features.minimum_clock_seconds_remaining < 10 THEN '5-9.9 seconds'
            WHEN features.minimum_clock_seconds_remaining < 30 THEN '10-29.9 seconds'
            ELSE '30 or more seconds'
        END AS min_clock_bucket,
        CASE
            WHEN features.minimum_clock_seconds_remaining IS NULL THEN 5
            WHEN features.minimum_clock_seconds_remaining < 5 THEN 1
            WHEN features.minimum_clock_seconds_remaining < 10 THEN 2
            WHEN features.minimum_clock_seconds_remaining < 30 THEN 3
            ELSE 4
        END AS min_clock_bucket_order
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched` AS games
    INNER JOIN `bruin-playground-arsalan.chess_peterthiel.stage_game_move_features` AS features USING (game_url)
    INNER JOIN dominant_time_control
        ON games.time_control = dominant_time_control.time_control
)

SELECT
    min_clock_bucket,
    min_clock_bucket_order,
    COUNT(*) AS games,
    100.0 * AVG(actual_score) AS score_rate_pct,
    100.0 * AVG(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END) AS win_rate_pct,
    100.0 * AVG(CASE WHEN player_result IN ('timeout', 'abandoned') THEN 1 ELSE 0 END) AS timeout_or_abandon_pct,
    AVG(clock_pressure_move_rate_pct) AS average_under_10_seconds_move_pct
FROM clock_labeled
GROUP BY min_clock_bucket, min_clock_bucket_order
ORDER BY min_clock_bucket_order
