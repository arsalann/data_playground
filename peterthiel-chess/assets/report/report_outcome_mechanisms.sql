/* @bruin
name: chess_peterthiel.report_outcome_mechanisms
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Summarizes Chess.com's recorded game-result mechanisms for the dominant time
  control. It separates resignation, timeout, checkmate, and draw mechanisms
  without inferring why an individual game ended.

depends:
  - chess_peterthiel.stage_games_enriched

materialization:
  type: table
  strategy: create+replace

columns:
  - name: player_result
    type: VARCHAR
    description: Chess.com result mechanism from the target-player perspective.
    primary_key: true
    nullable: false
  - name: games
    type: INTEGER
    description: Number of dominant-time-control games with the result mechanism.
  - name: games_pct
    type: DOUBLE
    description: Percentage of dominant-time-control games with the result mechanism.
  - name: score_rate_pct
    type: DOUBLE
    description: Percentage of possible conventional score for the result mechanism.

@bruin */

WITH dominant_time_control AS (
    SELECT time_control
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched`
    GROUP BY time_control
    QUALIFY ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, time_control) = 1
)

SELECT
    player_result,
    COUNT(*) AS games,
    100.0 * SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER ()) AS games_pct,
    100.0 * AVG(actual_score) AS score_rate_pct
FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched`
INNER JOIN dominant_time_control USING (time_control)
GROUP BY player_result
ORDER BY games DESC, player_result
