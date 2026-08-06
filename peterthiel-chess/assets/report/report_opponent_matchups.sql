/* @bruin
name: chess_peterthiel.report_opponent_matchups
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Creates a repeat-opponent table for the dominant time control. Opponent
  ratings are public post-game snapshots and any small head-to-head sample is
  left visible so readers can assess reliability.

depends:
  - chess_peterthiel.stage_games_enriched

materialization:
  type: table
  strategy: create+replace

columns:
  - name: opponent_username
    type: VARCHAR
    description: Opponent public Chess.com username in a repeated matchup.
    primary_key: true
    nullable: false
  - name: games
    type: INTEGER
    description: Number of dominant-time-control public games against the opponent.
  - name: wins
    type: INTEGER
    description: Target-player wins against the opponent.
  - name: draws
    type: INTEGER
    description: Target-player draws against the opponent.
  - name: losses
    type: INTEGER
    description: Target-player losses against the opponent.
  - name: score_rate_pct
    type: DOUBLE
    description: Percentage of possible conventional score against the opponent.
  - name: average_opponent_rating_after
    type: DOUBLE
    description: Mean opponent public post-game rating snapshot in the matchup.
  - name: highest_opponent_rating_after
    type: INTEGER
    description: Highest opponent public post-game rating snapshot observed in the matchup.
  - name: top_rated_close_peer_games
    type: INTEGER
    description: Games against the opponent meeting the top-decile encountered close-rating-peer definition.

@bruin */

WITH dominant_time_control AS (
    SELECT time_control
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched`
    GROUP BY time_control
    QUALIFY ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, time_control) = 1
)

SELECT
    opponent_username,
    COUNT(*) AS games,
    COUNTIF(outcome = 'win') AS wins,
    COUNTIF(outcome = 'draw') AS draws,
    COUNTIF(outcome = 'loss') AS losses,
    100.0 * AVG(actual_score) AS score_rate_pct,
    AVG(opponent_rating_after) AS average_opponent_rating_after,
    MAX(opponent_rating_after) AS highest_opponent_rating_after,
    COUNTIF(is_top_rated_close_peer) AS top_rated_close_peer_games
FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched`
INNER JOIN dominant_time_control USING (time_control)
GROUP BY opponent_username
ORDER BY games DESC, opponent_username
