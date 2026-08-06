/* @bruin
name: chess_peterthiel.report_first_move_repertoire
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Summarizes the public account's first move as White and first reply as Black
  in the dominant time control. This is a readable opening-repertoire measure
  derived directly from parsed SAN rather than an engine assessment.

depends:
  - chess_peterthiel.stage_games_enriched
  - chess_peterthiel.stage_game_moves

materialization:
  type: table
  strategy: create+replace

columns:
  - name: player_color
    type: VARCHAR
    description: Color played by the target account for the first-move observation.
    primary_key: true
    nullable: false
  - name: first_move
    type: VARCHAR
    description: Standard algebraic notation for the target account's first move.
    primary_key: true
    nullable: false
  - name: move_label
    type: VARCHAR
    description: Readable category joining player color and standard algebraic first move.
  - name: first_move_rank
    type: INTEGER
    description: Usage rank of the first move within the player color.
  - name: games
    type: INTEGER
    description: Number of dominant-time-control games using the first move.
  - name: usage_pct
    type: DOUBLE
    description: Percentage of color-specific games using the first move.
  - name: score_rate_pct
    type: DOUBLE
    description: Percentage of possible conventional score earned in games using the first move.

@bruin */

WITH dominant_time_control AS (
    SELECT time_control
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched`
    GROUP BY time_control
    QUALIFY ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, time_control) = 1
),

first_moves AS (
    SELECT
        moves.player_color,
        moves.san AS first_move,
        COUNT(*) AS games,
        100.0 * AVG(games.actual_score) AS score_rate_pct
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_game_moves` AS moves
    INNER JOIN `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched` AS games USING (game_url)
    INNER JOIN dominant_time_control
        ON games.time_control = dominant_time_control.time_control
    WHERE moves.move_number = 1
    GROUP BY moves.player_color, moves.san
),

with_totals AS (
    SELECT
        *,
        SUM(games) OVER (PARTITION BY player_color) AS color_games,
        ROW_NUMBER() OVER (
            PARTITION BY player_color
            ORDER BY games DESC, first_move
        ) AS first_move_rank
    FROM first_moves
)

SELECT
    player_color,
    first_move,
    CONCAT(INITCAP(player_color), ' ', first_move) AS move_label,
    first_move_rank,
    games,
    100.0 * SAFE_DIVIDE(games, color_games) AS usage_pct,
    score_rate_pct
FROM with_totals
ORDER BY player_color, first_move_rank
