/* @bruin
name: chess_peterthiel.stage_player_activity_streaks
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Identifies consecutive UTC calendar dates with at least one public Chess.com
  game. This makes the longest active-day streak auditable while clearly
  distinguishing it from a continuous real-world playing streak.

depends:
  - chess_peterthiel.stage_games_enriched

materialization:
  type: table
  strategy: create+replace

columns:
  - name: activity_streak_id
    type: VARCHAR
    description: Stable sequential identifier for a run of consecutive active UTC dates.
    primary_key: true
    nullable: false
  - name: activity_streak_number
    type: INTEGER
    description: Chronological ordinal of the active-day streak across the available public archive.
  - name: streak_start_date_utc
    type: DATE
    description: First active UTC date in the streak.
  - name: streak_end_date_utc
    type: DATE
    description: Final active UTC date in the streak.
  - name: consecutive_active_days
    type: INTEGER
    description: Count of consecutive UTC calendar dates with at least one recorded game.
  - name: games
    type: INTEGER
    description: Total public games played across the active-day streak.
  - name: score
    type: DOUBLE
    description: Sum of conventional target-player game scores across the streak.
  - name: score_rate_pct
    type: DOUBLE
    description: Percentage of possible game score earned across the streak.

@bruin */

WITH daily_activity AS (
    SELECT
        game_date_utc,
        COUNT(*) AS games,
        SUM(actual_score) AS score
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched`
    GROUP BY game_date_utc
),

with_prior_day AS (
    SELECT
        *,
        LAG(game_date_utc) OVER (ORDER BY game_date_utc) AS prior_active_date_utc
    FROM daily_activity
),

streak_boundaries AS (
    SELECT
        *,
        CASE
            WHEN prior_active_date_utc IS NULL THEN 1
            WHEN DATE_DIFF(game_date_utc, prior_active_date_utc, DAY) > 1 THEN 1
            ELSE 0
        END AS is_new_streak
    FROM with_prior_day
),

streak_labeled AS (
    SELECT
        *,
        SUM(is_new_streak) OVER (
            ORDER BY game_date_utc
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS activity_streak_number
    FROM streak_boundaries
)

SELECT
    CONCAT('streak_', CAST(activity_streak_number AS STRING)) AS activity_streak_id,
    activity_streak_number,
    MIN(game_date_utc) AS streak_start_date_utc,
    MAX(game_date_utc) AS streak_end_date_utc,
    COUNT(*) AS consecutive_active_days,
    SUM(games) AS games,
    SUM(score) AS score,
    100.0 * SAFE_DIVIDE(SUM(score), SUM(games)) AS score_rate_pct
FROM streak_labeled
GROUP BY activity_streak_number
ORDER BY activity_streak_number
