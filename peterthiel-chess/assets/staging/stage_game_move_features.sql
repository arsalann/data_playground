/* @bruin
name: chess_peterthiel.stage_game_move_features
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Aggregates PGN-derived target-player moves into game-level descriptive
  features. The table supports analyses of game length, capture and check
  frequency, castling, promotion, move-phase mix, and public clock behavior.

  Rates normalize actions to 40 target-player moves so games of different
  lengths can be compared. They are style proxies, not chess-engine evidence
  of strategic quality or intent.

depends:
  - chess_peterthiel.stage_games_enriched
  - chess_peterthiel.stage_game_moves

materialization:
  type: table
  strategy: create+replace

columns:
  - name: game_url
    type: VARCHAR
    description: Public Chess.com URL for the game; stable game-level natural key.
    primary_key: true
    nullable: false
  - name: game_uuid
    type: VARCHAR
    description: Chess.com UUID for the game.
  - name: has_parsed_moves
    type: BOOLEAN
    description: Whether at least one target-player move was parsed from the public PGN.
  - name: player_move_count
    type: INTEGER
    description: Number of target-player moves parsed from public PGN.
  - name: last_full_move_number
    type: INTEGER
    description: Last standard full-move number reached by the target player; zero when no moves were parsed.
  - name: capture_moves
    type: INTEGER
    description: Count of target-player SAN moves that record a capture.
  - name: check_moves
    type: INTEGER
    description: Count of target-player SAN moves that record check or checkmate.
  - name: checkmate_moves
    type: INTEGER
    description: Count of target-player SAN moves that record checkmate.
  - name: kingside_castles
    type: INTEGER
    description: Count of target-player king-side castling moves.
  - name: queenside_castles
    type: INTEGER
    description: Count of target-player queen-side castling moves.
  - name: promotion_moves
    type: INTEGER
    description: Count of target-player pawn-promotion moves.
  - name: pawn_moves
    type: INTEGER
    description: Count of target-player moves inferred as pawn moves from SAN.
  - name: queen_moves
    type: INTEGER
    description: Count of target-player moves inferred as queen moves from SAN.
  - name: early_queen_moves
    type: INTEGER
    description: Count of target-player queen moves in full moves 1 through 10.
  - name: capture_moves_per_40
    type: DOUBLE
    description: Target-player capture moves normalized to 40 target-player moves.
  - name: check_moves_per_40
    type: DOUBLE
    description: Target-player checking moves normalized to 40 target-player moves.
  - name: pawn_moves_per_40
    type: DOUBLE
    description: Target-player pawn moves normalized to 40 target-player moves.
  - name: clock_annotated_moves
    type: INTEGER
    description: Number of target-player moves with a public remaining-clock annotation.
  - name: clock_annotation_coverage_pct
    type: DOUBLE
    description: Percentage of parsed target-player moves with a public clock annotation.
  - name: mean_clock_seconds_remaining
    type: DOUBLE
    description: Mean target-player remaining clock in seconds after annotated moves.
  - name: minimum_clock_seconds_remaining
    type: DOUBLE
    description: Minimum target-player remaining clock in seconds after annotated moves.
  - name: mean_estimated_think_seconds
    type: DOUBLE
    description: Mean estimated move elapsed time from public clock annotations, adjusted for time-control increment.
  - name: p90_estimated_think_seconds
    type: DOUBLE
    description: 90th percentile estimated move elapsed time from public clock annotations.
  - name: clock_pressure_moves_under_10_seconds
    type: INTEGER
    description: Count of target-player moves ending with under ten seconds on the public clock annotation.
  - name: clock_pressure_move_rate_pct
    type: DOUBLE
    description: Percentage of annotated target-player moves ending with under ten seconds remaining.

@bruin */

WITH move_aggregates AS (
    SELECT
        game_url,
        COUNT(*) AS player_move_count,
        MAX(move_number) AS last_full_move_number,
        COUNTIF(is_capture) AS capture_moves,
        COUNTIF(is_check) AS check_moves,
        COUNTIF(is_checkmate) AS checkmate_moves,
        COUNTIF(is_castle_kingside) AS kingside_castles,
        COUNTIF(is_castle_queenside) AS queenside_castles,
        COUNTIF(is_promotion) AS promotion_moves,
        COUNTIF(piece_type = 'pawn') AS pawn_moves,
        COUNTIF(piece_type = 'queen') AS queen_moves,
        COUNTIF(piece_type = 'queen' AND move_number <= 10) AS early_queen_moves,
        COUNTIF(clock_seconds_remaining IS NOT NULL) AS clock_annotated_moves,
        AVG(clock_seconds_remaining) AS mean_clock_seconds_remaining,
        MIN(clock_seconds_remaining) AS minimum_clock_seconds_remaining,
        AVG(estimated_think_seconds) AS mean_estimated_think_seconds,
        APPROX_QUANTILES(estimated_think_seconds, 100)[SAFE_OFFSET(90)] AS p90_estimated_think_seconds,
        COUNTIF(is_clock_pressure_under_10_seconds) AS clock_pressure_moves_under_10_seconds
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_game_moves`
    GROUP BY game_url
)

SELECT
    games.game_url,
    games.game_uuid,
    aggregates.game_url IS NOT NULL AS has_parsed_moves,
    COALESCE(aggregates.player_move_count, 0) AS player_move_count,
    COALESCE(aggregates.last_full_move_number, 0) AS last_full_move_number,
    COALESCE(aggregates.capture_moves, 0) AS capture_moves,
    COALESCE(aggregates.check_moves, 0) AS check_moves,
    COALESCE(aggregates.checkmate_moves, 0) AS checkmate_moves,
    COALESCE(aggregates.kingside_castles, 0) AS kingside_castles,
    COALESCE(aggregates.queenside_castles, 0) AS queenside_castles,
    COALESCE(aggregates.promotion_moves, 0) AS promotion_moves,
    COALESCE(aggregates.pawn_moves, 0) AS pawn_moves,
    COALESCE(aggregates.queen_moves, 0) AS queen_moves,
    COALESCE(aggregates.early_queen_moves, 0) AS early_queen_moves,
    SAFE_DIVIDE(40.0 * COALESCE(aggregates.capture_moves, 0), NULLIF(aggregates.player_move_count, 0)) AS capture_moves_per_40,
    SAFE_DIVIDE(40.0 * COALESCE(aggregates.check_moves, 0), NULLIF(aggregates.player_move_count, 0)) AS check_moves_per_40,
    SAFE_DIVIDE(40.0 * COALESCE(aggregates.pawn_moves, 0), NULLIF(aggregates.player_move_count, 0)) AS pawn_moves_per_40,
    COALESCE(aggregates.clock_annotated_moves, 0) AS clock_annotated_moves,
    100.0 * SAFE_DIVIDE(aggregates.clock_annotated_moves, NULLIF(aggregates.player_move_count, 0)) AS clock_annotation_coverage_pct,
    aggregates.mean_clock_seconds_remaining,
    aggregates.minimum_clock_seconds_remaining,
    aggregates.mean_estimated_think_seconds,
    aggregates.p90_estimated_think_seconds,
    COALESCE(aggregates.clock_pressure_moves_under_10_seconds, 0) AS clock_pressure_moves_under_10_seconds,
    100.0 * SAFE_DIVIDE(
        aggregates.clock_pressure_moves_under_10_seconds,
        NULLIF(aggregates.clock_annotated_moves, 0)
    ) AS clock_pressure_move_rate_pct
FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched` AS games
LEFT JOIN move_aggregates AS aggregates USING (game_url)
ORDER BY games.game_end_time_utc, games.game_uuid
