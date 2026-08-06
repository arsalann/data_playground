/* @bruin
name: chess_peterthiel.report_move_style_by_bucket
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Aggregates PGN-derived move signatures by descriptive move-number bucket for
  the dominant time control. Capture, check, and pawn-move rates are normalized
  to 40 target-player moves to enable phase comparisons.

  The output is descriptive and does not infer chess quality, strategy quality,
  or causal mechanisms from SAN notation alone.

depends:
  - chess_peterthiel.stage_games_enriched
  - chess_peterthiel.stage_game_moves

materialization:
  type: table
  strategy: create+replace

columns:
  - name: move_bucket
    type: VARCHAR
    description: Machine-readable descriptive move-number bucket.
    primary_key: true
    nullable: false
  - name: move_bucket_label
    type: VARCHAR
    description: Human-readable move-number bucket label.
  - name: move_bucket_order
    type: INTEGER
    description: Natural chronological display order for the move buckets.
  - name: player_moves
    type: INTEGER
    description: Number of target-player moves in the bucket.
  - name: capture_moves_per_40
    type: DOUBLE
    description: Capture moves normalized to 40 target-player moves in the bucket.
  - name: check_moves_per_40
    type: DOUBLE
    description: Checking moves normalized to 40 target-player moves in the bucket.
  - name: pawn_moves_per_40
    type: DOUBLE
    description: Pawn moves normalized to 40 target-player moves in the bucket.
  - name: avg_estimated_think_seconds
    type: DOUBLE
    description: Mean estimated elapsed move time from public clock annotations in seconds.
  - name: under_10_seconds_move_pct
    type: DOUBLE
    description: Percentage of moves in the bucket ending below ten seconds on public clock annotations.

@bruin */

WITH dominant_time_control AS (
    SELECT time_control
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched`
    GROUP BY time_control
    QUALIFY ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, time_control) = 1
)

SELECT
    moves.move_bucket,
    CASE moves.move_bucket
        WHEN 'moves_1_10' THEN 'Moves 1-10'
        WHEN 'moves_11_30' THEN 'Moves 11-30'
        ELSE 'Moves 31+'
    END AS move_bucket_label,
    CASE moves.move_bucket
        WHEN 'moves_1_10' THEN 1
        WHEN 'moves_11_30' THEN 2
        ELSE 3
    END AS move_bucket_order,
    COUNT(*) AS player_moves,
    40.0 * SAFE_DIVIDE(COUNTIF(moves.is_capture), COUNT(*)) AS capture_moves_per_40,
    40.0 * SAFE_DIVIDE(COUNTIF(moves.is_check), COUNT(*)) AS check_moves_per_40,
    40.0 * SAFE_DIVIDE(COUNTIF(moves.piece_type = 'pawn'), COUNT(*)) AS pawn_moves_per_40,
    AVG(moves.estimated_think_seconds) AS avg_estimated_think_seconds,
    100.0 * AVG(CASE WHEN moves.clock_seconds_remaining < 10 THEN 1 ELSE 0 END) AS under_10_seconds_move_pct
FROM `bruin-playground-arsalan.chess_peterthiel.stage_game_moves` AS moves
INNER JOIN `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched` AS games USING (game_url)
INNER JOIN dominant_time_control
    ON games.time_control = dominant_time_control.time_control
GROUP BY moves.move_bucket
ORDER BY move_bucket_order
