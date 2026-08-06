/* @bruin
name: chess_peterthiel.report_monthly_performance
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Produces monthly game volume, score, opponent-rating, and month-end rating
  summaries by Chess.com time control for the overview report.

  Rating values are public post-game snapshots. A monthly end value is the last
  available post-game rating in that month, not an independently verified rating history.

depends:
  - chess_peterthiel.stage_games_enriched

materialization:
  type: table
  strategy: create+replace

columns:
  - name: game_month
    type: DATE
    description: First UTC date of the month represented by the summary row.
    primary_key: true
    nullable: false
  - name: time_control
    type: VARCHAR
    description: Chess.com time-control string represented by the summary row.
    primary_key: true
    nullable: false
  - name: games
    type: INTEGER
    description: Number of public games in the month and time control.
  - name: score_rate_pct
    type: DOUBLE
    description: Percentage of possible conventional score earned in the month and time control.
  - name: month_end_rating_after
    type: INTEGER
    description: Last public post-game rating snapshot observed in the month and time control.
  - name: average_opponent_rating_after
    type: DOUBLE
    description: Mean opponent post-game rating snapshot in the month and time control.
  - name: close_peer_games
    type: INTEGER
    description: Games within 100 public post-game rating points in the month and time control.
  - name: close_peer_score_rate_pct
    type: DOUBLE
    description: Percentage of possible score earned in the month's public close-rating peer games.

@bruin */

SELECT
    game_month,
    time_control,
    COUNT(*) AS games,
    100.0 * AVG(actual_score) AS score_rate_pct,
    ARRAY_AGG(player_rating_after ORDER BY game_end_time_utc DESC, game_uuid DESC LIMIT 1)[SAFE_OFFSET(0)] AS month_end_rating_after,
    AVG(opponent_rating_after) AS average_opponent_rating_after,
    COUNTIF(is_postgame_close_rating_peer) AS close_peer_games,
    100.0 * AVG(IF(is_postgame_close_rating_peer, actual_score, NULL)) AS close_peer_score_rate_pct
FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched`
GROUP BY game_month, time_control
ORDER BY game_month, time_control
