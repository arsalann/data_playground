/* @bruin
name: chess_peterthiel.report_peer_competition
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Summarizes performance in two transparent peer cohorts for the dominant
  time control: all opponents within 100 public post-game rating points and
  the subset who are also in the top decile of opponent ratings encountered.

  The expected-score field is a proxy because Chess.com supplies post-game
  ratings. It reconstructs the target player's prior rating within the time
  control but uses the opponent's public post-game rating snapshot.

depends:
  - chess_peterthiel.stage_games_enriched

materialization:
  type: table
  strategy: create+replace

columns:
  - name: cohort_key
    type: VARCHAR
    description: Machine-readable peer cohort identifier.
    primary_key: true
    nullable: false
  - name: cohort_label
    type: VARCHAR
    description: Human-readable peer cohort label.
  - name: cohort_order
    type: INTEGER
    description: Display ordering for the two peer cohorts.
  - name: time_control
    type: VARCHAR
    description: Dominant Chess.com time-control string used for this comparison.
  - name: games
    type: INTEGER
    description: Number of public games in the cohort.
  - name: actual_score_pct
    type: DOUBLE
    description: Percentage of possible conventional score earned in the cohort.
  - name: expected_score_proxy_pct
    type: DOUBLE
    description: Mean rating-derived expected-score proxy in percent for the cohort.
  - name: performance_residual_proxy_pp
    type: DOUBLE
    description: Actual score minus expected-score proxy in percentage points.
  - name: average_player_rating_before
    type: DOUBLE
    description: Mean reconstructed target-player pre-game rating proxy in the cohort.
  - name: average_opponent_rating_after
    type: DOUBLE
    description: Mean opponent post-game rating snapshot in the cohort.
  - name: top_opponent_rating_p90
    type: INTEGER
    description: Top-decile encountered-opponent rating threshold for the time control.
  - name: even_score_pct
    type: DOUBLE
    description: Constant 50 percent reference line for score interpretation.

@bruin */

WITH dominant_time_control AS (
    SELECT time_control
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched`
    GROUP BY time_control
    QUALIFY ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, time_control) = 1
),

dominant_games AS (
    SELECT games.*
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched` AS games
    INNER JOIN dominant_time_control USING (time_control)
)

SELECT
    'all_close_rating_peers' AS cohort_key,
    'All close-rating peers' AS cohort_label,
    1 AS cohort_order,
    ANY_VALUE(time_control) AS time_control,
    COUNT(*) AS games,
    100.0 * AVG(actual_score) AS actual_score_pct,
    100.0 * AVG(expected_score_proxy) AS expected_score_proxy_pct,
    100.0 * AVG(performance_residual_proxy) AS performance_residual_proxy_pp,
    AVG(player_rating_before) AS average_player_rating_before,
    AVG(opponent_rating_after) AS average_opponent_rating_after,
    MAX(top_opponent_rating_p90) AS top_opponent_rating_p90,
    50.0 AS even_score_pct
FROM dominant_games
WHERE is_postgame_close_rating_peer

UNION ALL

SELECT
    'top_rated_close_peers' AS cohort_key,
    'Top-rated close peers encountered' AS cohort_label,
    2 AS cohort_order,
    ANY_VALUE(time_control) AS time_control,
    COUNT(*) AS games,
    100.0 * AVG(actual_score) AS actual_score_pct,
    100.0 * AVG(expected_score_proxy) AS expected_score_proxy_pct,
    100.0 * AVG(performance_residual_proxy) AS performance_residual_proxy_pp,
    AVG(player_rating_before) AS average_player_rating_before,
    AVG(opponent_rating_after) AS average_opponent_rating_after,
    MAX(top_opponent_rating_p90) AS top_opponent_rating_p90,
    50.0 AS even_score_pct
FROM dominant_games
WHERE is_top_rated_close_peer
