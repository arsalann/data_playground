/* @bruin
name: chess_peterthiel.stage_game_moves
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Expands public PGN records into one row for each move made by the @peterthiel
  account. The transformation extracts standard algebraic notation, move order,
  descriptive tactical markers, castling and promotion flags, and remaining
  clock values from Chess.com's PGN clock annotations.

  These are descriptive move-style signals rather than a chess-engine judgment
  of move quality. Clock-derived elapsed time is an estimate from sequential
  remaining-clock annotations and is nullable when the public PGN omits clocks.

  Source: https://www.chess.com/news/view/published-data-api

depends:
  - chess_peterthiel.raw_peterthiel_games
  - chess_peterthiel.stage_games_enriched

materialization:
  type: table
  strategy: create+replace

columns:
  - name: move_id
    type: VARCHAR
    description: Stable synthetic key formed from the public game UUID and ply number.
    primary_key: true
    nullable: false
  - name: game_url
    type: VARCHAR
    description: Public Chess.com URL for the parent game.
  - name: game_uuid
    type: VARCHAR
    description: Chess.com UUID for the parent game.
  - name: game_end_time_utc
    type: TIMESTAMP
    description: UTC timestamp at which the parent game ended.
  - name: game_date_utc
    type: DATE
    description: UTC calendar date of the parent game.
  - name: time_class
    type: VARCHAR
    description: Chess.com speed category of the parent game.
  - name: time_control
    type: VARCHAR
    description: Chess.com time-control string of the parent game.
  - name: player_color
    type: VARCHAR
    description: Color played by the target account in the parent game.
  - name: ply_number
    type: INTEGER
    description: One-based half-move index across both players in the public PGN.
  - name: move_number
    type: INTEGER
    description: Standard full-move number corresponding to this move.
  - name: player_move_index
    type: INTEGER
    description: One-based sequential index of the target account's moves within the game.
  - name: san
    type: VARCHAR
    description: Standard algebraic notation token for the target account's move.
  - name: piece_type
    type: VARCHAR
    description: Piece inferred from SAN as pawn, knight, bishop, rook, queen, or king.
  - name: move_bucket
    type: VARCHAR
    description: "Descriptive move-number bucket: moves 1-10, 11-30, or 31-plus."
  - name: is_capture
    type: BOOLEAN
    description: Whether SAN records a capture on this move.
  - name: is_check
    type: BOOLEAN
    description: Whether SAN records that the move gives check or checkmate.
  - name: is_checkmate
    type: BOOLEAN
    description: Whether SAN records checkmate on this move.
  - name: is_castle_kingside
    type: BOOLEAN
    description: Whether the move is a king-side castle.
  - name: is_castle_queenside
    type: BOOLEAN
    description: Whether the move is a queen-side castle.
  - name: is_promotion
    type: BOOLEAN
    description: Whether SAN records a pawn promotion.
  - name: clock_seconds_remaining
    type: DOUBLE
    description: Target player's remaining clock in seconds from the public PGN annotation after the move.
  - name: estimated_think_seconds
    type: DOUBLE
    description: Estimated elapsed move time from successive public clock annotations adjusted for increment; not a direct Chess.com timing field.
  - name: is_clock_pressure_under_10_seconds
    type: BOOLEAN
    description: Whether the public remaining-clock annotation is below ten seconds after the move.
  - name: is_valid_san
    type: BOOLEAN
    description: Whether the token matches the supported standard-algebraic-notation parsing pattern.

@bruin */

WITH raw_games AS (
    SELECT * EXCEPT (row_num)
    FROM (
        SELECT
            url AS game_url,
            pgn,
            ROW_NUMBER() OVER (
                PARTITION BY COALESCE(url, uuid)
                ORDER BY _ingestr_loaded_at DESC
            ) AS row_num
        FROM `bruin-playground-arsalan.chess_peterthiel.raw_peterthiel_games`
        WHERE url IS NOT NULL
    )
    WHERE row_num = 1
),

pgn_games AS (
    SELECT
        games.game_url,
        games.game_uuid,
        games.game_end_time_utc,
        games.game_date_utc,
        games.time_class,
        games.time_control,
        games.player_color,
        games.base_time_seconds,
        games.increment_seconds,
        raw_games.pgn,
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(raw_games.pgn, r'(?s)^.*?\n\n', ''),
                    r'\{[^}]*\}',
                    ' '
                ),
                r'\d+\.(?:\.\.)?',
                ' '
            ),
            r'(1-0|0-1|1/2-1/2|\*)',
            ' '
        ) AS move_text
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched` AS games
    INNER JOIN raw_games USING (game_url)
    WHERE games.has_pgn
),

all_moves AS (
    SELECT
        game_url,
        game_uuid,
        game_end_time_utc,
        game_date_utc,
        time_class,
        time_control,
        player_color,
        base_time_seconds,
        increment_seconds,
        ply_offset + 1 AS ply_number,
        san
    FROM pgn_games
    CROSS JOIN UNNEST(
        ARRAY(
            SELECT TRIM(token)
            FROM UNNEST(SPLIT(move_text, ' ')) AS token
            WHERE TRIM(token) != ''
        )
    ) AS san WITH OFFSET AS ply_offset
),

clock_annotations AS (
    SELECT
        game_url,
        ply_offset + 1 AS ply_number,
        SAFE_CAST(SPLIT(clock_text, ':')[SAFE_OFFSET(0)] AS FLOAT64) * 3600
            + SAFE_CAST(SPLIT(clock_text, ':')[SAFE_OFFSET(1)] AS FLOAT64) * 60
            + SAFE_CAST(SPLIT(clock_text, ':')[SAFE_OFFSET(2)] AS FLOAT64) AS clock_seconds_remaining
    FROM pgn_games
    CROSS JOIN UNNEST(
        REGEXP_EXTRACT_ALL(pgn, r'\[%clk ([0-9]+:[0-9]{2}:[0-9]+(?:\.[0-9]+)?)\]')
    ) AS clock_text WITH OFFSET AS ply_offset
),

player_moves AS (
    SELECT
        moves.*,
        CAST(CEIL(moves.ply_number / 2.0) AS INT64) AS move_number,
        clocks.clock_seconds_remaining,
        ROW_NUMBER() OVER (
            PARTITION BY moves.game_url
            ORDER BY moves.ply_number
        ) AS player_move_index
    FROM all_moves AS moves
    LEFT JOIN clock_annotations AS clocks USING (game_url, ply_number)
    WHERE (moves.player_color = 'white' AND MOD(moves.ply_number, 2) = 1)
        OR (moves.player_color = 'black' AND MOD(moves.ply_number, 2) = 0)
),

with_prior_clock AS (
    SELECT
        *,
        LAG(clock_seconds_remaining) OVER (
            PARTITION BY game_url
            ORDER BY player_move_index
        ) AS prior_clock_seconds_remaining
    FROM player_moves
)

SELECT
    CONCAT(game_uuid, '_', CAST(ply_number AS STRING)) AS move_id,
    game_url,
    game_uuid,
    game_end_time_utc,
    game_date_utc,
    time_class,
    time_control,
    player_color,
    ply_number,
    move_number,
    player_move_index,
    san,
    CASE
        WHEN REGEXP_CONTAINS(san, r'^O-O') OR REGEXP_CONTAINS(san, r'^K') THEN 'king'
        WHEN REGEXP_CONTAINS(san, r'^Q') THEN 'queen'
        WHEN REGEXP_CONTAINS(san, r'^R') THEN 'rook'
        WHEN REGEXP_CONTAINS(san, r'^B') THEN 'bishop'
        WHEN REGEXP_CONTAINS(san, r'^N') THEN 'knight'
        ELSE 'pawn'
    END AS piece_type,
    CASE
        WHEN move_number <= 10 THEN 'moves_1_10'
        WHEN move_number <= 30 THEN 'moves_11_30'
        ELSE 'moves_31_plus'
    END AS move_bucket,
    STRPOS(san, 'x') > 0 AS is_capture,
    REGEXP_CONTAINS(san, r'[+#]$') AS is_check,
    ENDS_WITH(san, '#') AS is_checkmate,
    REGEXP_CONTAINS(san, r'^O-O[+#]?$') AS is_castle_kingside,
    REGEXP_CONTAINS(san, r'^O-O-O[+#]?$') AS is_castle_queenside,
    STRPOS(san, '=') > 0 AS is_promotion,
    clock_seconds_remaining,
    CASE
        WHEN clock_seconds_remaining IS NOT NULL AND base_time_seconds IS NOT NULL
        THEN GREATEST(
            COALESCE(prior_clock_seconds_remaining, CAST(base_time_seconds AS FLOAT64))
                - clock_seconds_remaining + increment_seconds,
            0.0
        )
        ELSE NULL
    END AS estimated_think_seconds,
    clock_seconds_remaining < 10 AS is_clock_pressure_under_10_seconds,
    REGEXP_CONTAINS(
        san,
        r'^(O-O(?:-O)?[+#]?|[KQRBN]?[a-h]?[1-8]?(?:x?[a-h][1-8])?(?:=[QRBN])?[+#]?)$'
    ) AS is_valid_san
FROM with_prior_clock
ORDER BY game_end_time_utc, game_uuid, ply_number
