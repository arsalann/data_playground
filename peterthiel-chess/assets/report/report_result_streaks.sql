/* @bruin
name: chess_peterthiel.report_result_streaks
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Records the five longest uninterrupted winning and losing runs in the public
  account's dominant time control. A draw breaks a run. This is a result
  sequence summary; it does not diagnose momentum, psychology, or cause.

depends:
  - chess_peterthiel.stage_games_enriched

materialization:
  type: table
  strategy: create+replace

columns:
  - name: streak_kind
    type: VARCHAR
    description: Whether the uninterrupted public result run consists of wins or losses.
    primary_key: true
    nullable: false
  - name: streak_rank
    type: INTEGER
    description: Descending rank by consecutive games within the streak kind.
    primary_key: true
    nullable: false
  - name: consecutive_games
    type: INTEGER
    description: Number of consecutive public wins or losses with draws breaking the run.
  - name: streak_start_utc
    type: TIMESTAMP
    description: Recorded UTC end time of the first game in the result run.
  - name: streak_end_utc
    type: TIMESTAMP
    description: Recorded UTC end time of the final game in the result run.
  - name: streak_duration_hours
    type: DOUBLE
    description: Elapsed hours from first to final recorded game end time in the run.
  - name: player_rating_before_start
    type: INTEGER
    description: Reconstructed player pre-game rating proxy for the first game in the run.
  - name: player_rating_after_end
    type: INTEGER
    description: Public player post-game rating snapshot for the final game in the run.
  - name: rating_change_proxy
    type: INTEGER
    description: Final post-game rating minus first-game reconstructed pre-game rating proxy.
  - name: first_game_url
    type: VARCHAR
    description: Public URL of the first game in the run for independent inspection.

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
        LAG(outcome) OVER (
            ORDER BY game_end_time_utc, game_uuid
        ) AS prior_outcome
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched` AS games
    INNER JOIN dominant_time_control USING (time_control)
),

run_labeled AS (
    SELECT
        *,
        SUM(
            CASE
                WHEN outcome NOT IN ('win', 'loss')
                    OR prior_outcome IS NULL
                    OR outcome != prior_outcome THEN 1
                ELSE 0
            END
        ) OVER (
            ORDER BY game_end_time_utc, game_uuid
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS run_group
    FROM ordered_games
),

runs AS (
    SELECT
        outcome AS streak_kind,
        run_group,
        COUNT(*) AS consecutive_games,
        MIN(game_end_time_utc) AS streak_start_utc,
        MAX(game_end_time_utc) AS streak_end_utc,
        TIMESTAMP_DIFF(
            MAX(game_end_time_utc),
            MIN(game_end_time_utc),
            SECOND
        ) / 3600.0 AS streak_duration_hours,
        ARRAY_AGG(player_rating_before ORDER BY game_end_time_utc, game_uuid LIMIT 1)[SAFE_OFFSET(0)] AS player_rating_before_start,
        ARRAY_AGG(player_rating_after ORDER BY game_end_time_utc DESC, game_uuid DESC LIMIT 1)[SAFE_OFFSET(0)] AS player_rating_after_end,
        ARRAY_AGG(game_url ORDER BY game_end_time_utc, game_uuid LIMIT 1)[SAFE_OFFSET(0)] AS first_game_url
    FROM run_labeled
    WHERE outcome IN ('win', 'loss')
    GROUP BY streak_kind, run_group
),

ranked_runs AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY streak_kind
            ORDER BY consecutive_games DESC, streak_start_utc
        ) AS streak_rank
    FROM runs
)

SELECT
    streak_kind,
    streak_rank,
    consecutive_games,
    streak_start_utc,
    streak_end_utc,
    streak_duration_hours,
    player_rating_before_start,
    player_rating_after_end,
    player_rating_after_end - player_rating_before_start AS rating_change_proxy,
    first_game_url
FROM ranked_runs
WHERE streak_rank <= 5
ORDER BY streak_kind, streak_rank
