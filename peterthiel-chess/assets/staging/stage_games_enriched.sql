/* @bruin
name: chess_peterthiel.stage_games_enriched
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Normalizes the public Chess.com archive into one row per game from the
  @peterthiel account's perspective. It retains the public result, time,
  opening, rating, clock-annotation, and accuracy signals needed for the
  downstream habit, peer, and move-style analyses.

  Chess.com game ratings are snapshots recorded after a game. The
  player_rating_before field therefore reconstructs the target account's
  pre-game rating as the prior game-end rating within the same time control;
  opponent_rating_after remains a post-game snapshot. Any expected-score
  metric is explicitly a proxy, not an official pre-game Elo calculation.

  Source: https://www.chess.com/news/view/published-data-api

depends:
  - chess_peterthiel.raw_peterthiel_games

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
    description: Chess.com UUID supplied with the public game record.
  - name: game_end_time_utc
    type: TIMESTAMP
    description: UTC timestamp at which Chess.com recorded the game as ending.
  - name: game_start_time_utc
    type: TIMESTAMP
    description: UTC start timestamp parsed from the PGN header when available.
  - name: game_duration_seconds
    type: INTEGER
    description: Elapsed seconds from parsed PGN start time to the recorded end time; nullable when unavailable.
  - name: game_date_utc
    type: DATE
    description: UTC calendar date on which the game ended.
  - name: game_year
    type: INTEGER
    description: UTC calendar year of the game end time.
  - name: game_month
    type: DATE
    description: First UTC calendar date of the game month.
  - name: game_hour_utc
    type: INTEGER
    description: UTC hour-of-day when the game ended, from 0 through 23.
  - name: day_of_week_utc
    type: INTEGER
    description: UTC weekday where 1 is Sunday and 7 is Saturday.
  - name: day_name_utc
    type: VARCHAR
    description: English UTC weekday label derived from the game end time.
  - name: player_color
    type: VARCHAR
    description: Color played by the public account, either white or black.
  - name: opponent_username
    type: VARCHAR
    description: Opponent's public Chess.com username as recorded for this game.
  - name: time_class
    type: VARCHAR
    description: Chess.com speed category, such as blitz or rapid.
  - name: time_control
    type: VARCHAR
    description: Chess.com time-control string, for example 180 for three minutes.
  - name: base_time_seconds
    type: INTEGER
    description: Initial clock allocation in seconds parsed from the time-control string.
  - name: increment_seconds
    type: INTEGER
    description: Per-move increment in seconds parsed from the time-control string; zero when none is encoded.
  - name: rated
    type: BOOLEAN
    description: Whether Chess.com marked the game as rated.
  - name: rules
    type: VARCHAR
    description: Chess.com ruleset identifier, expected to be standard chess for this archive.
  - name: player_result
    type: VARCHAR
    description: Chess.com result mechanism from the target player's perspective, for example win, resigned, or timeout.
  - name: outcome
    type: VARCHAR
    description: "Simplified result from the target perspective: win, draw, or loss."
  - name: actual_score
    type: DOUBLE
    description: "Conventional game score from the target perspective: win 1.0, draw 0.5, loss 0.0."
  - name: termination
    type: VARCHAR
    description: Free-text public PGN termination header, when supplied.
  - name: player_rating_after
    type: INTEGER
    description: Target account rating snapshot recorded after the game by Chess.com.
  - name: player_rating_before
    type: INTEGER
    description: Target account's prior game-end rating within the same time control, used as a pre-game rating proxy.
  - name: opponent_rating_after
    type: INTEGER
    description: Opponent rating snapshot recorded after the game by Chess.com.
  - name: postgame_rating_gap
    type: INTEGER
    description: Target post-game rating minus opponent post-game rating in rating points.
  - name: rating_gap_proxy
    type: INTEGER
    description: Target reconstructed pre-game rating proxy minus the opponent's post-game rating snapshot, in rating points.
  - name: expected_score_proxy
    type: DOUBLE
    description: Elo expected-score proxy from rating_gap_proxy; not an official pre-game expected score because opponent ratings are post-game snapshots.
  - name: performance_residual_proxy
    type: DOUBLE
    description: Actual score minus expected_score_proxy; positive values indicate a better result than the rating proxy expected.
  - name: is_postgame_close_rating_peer
    type: BOOLEAN
    description: Whether the two public post-game ratings differ by at most 100 points.
  - name: top_opponent_rating_p90
    type: INTEGER
    description: 90th percentile of opponent post-game rating observed in the same time control across this archive.
  - name: is_top_rated_opponent_encounter
    type: BOOLEAN
    description: Whether the opponent's recorded post-game rating is in the top decile of encountered opponents within the same time control.
  - name: is_top_rated_close_peer
    type: BOOLEAN
    description: Whether the game is both a post-game close-rating peer game and a top-decile encountered-opponent game.
  - name: opening_eco
    type: VARCHAR
    description: ECO code parsed from the public PGN header.
  - name: opening_name
    type: VARCHAR
    description: Human-readable opening slug parsed from Chess.com's public opening URL.
  - name: has_pgn
    type: BOOLEAN
    description: Whether public PGN text is present for the game.
  - name: has_clock_annotations
    type: BOOLEAN
    description: Whether the PGN contains at least one Chess.com clock annotation.
  - name: player_accuracy
    type: DOUBLE
    description: Chess.com-provided accuracy for the target player when available; coverage is incomplete and the field is not engine-recomputed here.
  - name: opponent_accuracy
    type: DOUBLE
    description: Chess.com-provided accuracy for the opponent when available; coverage is incomplete and the field is not engine-recomputed here.
  - name: has_accuracy
    type: BOOLEAN
    description: Whether Chess.com supplied the target player's accuracy for this game.
  - name: source_loaded_at
    type: TIMESTAMP
    description: Ingestr load timestamp for the raw source record.

@bruin */

WITH deduped AS (
    SELECT * EXCEPT (row_num)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY COALESCE(url, uuid)
                ORDER BY _ingestr_loaded_at DESC
            ) AS row_num
        FROM `bruin-playground-arsalan.chess_peterthiel.raw_peterthiel_games`
        WHERE url IS NOT NULL
    )
    WHERE row_num = 1
),

normalized AS (
    SELECT
        url AS game_url,
        uuid AS game_uuid,
        end_time AS game_end_time_utc,
        SAFE.PARSE_TIMESTAMP(
            '%Y.%m.%d %H:%M:%S',
            CONCAT(
                REGEXP_EXTRACT(pgn, r'\[UTCDate "([^"]+)"\]'),
                ' ',
                REGEXP_EXTRACT(pgn, r'\[UTCTime "([^"]+)"\]')
            )
        ) AS game_start_time_utc,
        DATE(end_time, 'UTC') AS game_date_utc,
        EXTRACT(YEAR FROM end_time AT TIME ZONE 'UTC') AS game_year,
        DATE_TRUNC(DATE(end_time, 'UTC'), MONTH) AS game_month,
        EXTRACT(HOUR FROM end_time AT TIME ZONE 'UTC') AS game_hour_utc,
        EXTRACT(DAYOFWEEK FROM end_time AT TIME ZONE 'UTC') AS day_of_week_utc,
        FORMAT_TIMESTAMP('%A', end_time, 'UTC') AS day_name_utc,
        CASE
            WHEN LOWER(JSON_VALUE(white, '$.username')) = 'peterthiel' THEN 'white'
            WHEN LOWER(JSON_VALUE(black, '$.username')) = 'peterthiel' THEN 'black'
            ELSE NULL
        END AS player_color,
        CASE
            WHEN LOWER(JSON_VALUE(white, '$.username')) = 'peterthiel' THEN JSON_VALUE(black, '$.username')
            WHEN LOWER(JSON_VALUE(black, '$.username')) = 'peterthiel' THEN JSON_VALUE(white, '$.username')
            ELSE NULL
        END AS opponent_username,
        time_class,
        time_control,
        SAFE_CAST(REGEXP_EXTRACT(time_control, r'^(\d+)') AS INT64) AS base_time_seconds,
        COALESCE(SAFE_CAST(REGEXP_EXTRACT(time_control, r'^\d+\+(\d+)$') AS INT64), 0) AS increment_seconds,
        rated,
        rules,
        CASE
            WHEN LOWER(JSON_VALUE(white, '$.username')) = 'peterthiel' THEN JSON_VALUE(white, '$.result')
            WHEN LOWER(JSON_VALUE(black, '$.username')) = 'peterthiel' THEN JSON_VALUE(black, '$.result')
            ELSE NULL
        END AS player_result,
        CASE
            WHEN LOWER(JSON_VALUE(white, '$.username')) = 'peterthiel' THEN SAFE_CAST(JSON_VALUE(white, '$.rating') AS INT64)
            WHEN LOWER(JSON_VALUE(black, '$.username')) = 'peterthiel' THEN SAFE_CAST(JSON_VALUE(black, '$.rating') AS INT64)
            ELSE NULL
        END AS player_rating_after,
        CASE
            WHEN LOWER(JSON_VALUE(white, '$.username')) = 'peterthiel' THEN SAFE_CAST(JSON_VALUE(black, '$.rating') AS INT64)
            WHEN LOWER(JSON_VALUE(black, '$.username')) = 'peterthiel' THEN SAFE_CAST(JSON_VALUE(white, '$.rating') AS INT64)
            ELSE NULL
        END AS opponent_rating_after,
        REGEXP_EXTRACT(pgn, r'\[Termination "([^"]+)"\]') AS termination,
        REGEXP_EXTRACT(pgn, r'\[ECO "([^"]+)"\]') AS opening_eco,
        INITCAP(REPLACE(REGEXP_EXTRACT(eco, r'/openings/([^?]+)$'), '-', ' ')) AS opening_name,
        pgn IS NOT NULL AND pgn != '' AS has_pgn,
        REGEXP_CONTAINS(pgn, r'\[%clk ') AS has_clock_annotations,
        CASE
            WHEN LOWER(JSON_VALUE(white, '$.username')) = 'peterthiel' THEN SAFE_CAST(JSON_VALUE(accuracies, '$.white') AS FLOAT64)
            WHEN LOWER(JSON_VALUE(black, '$.username')) = 'peterthiel' THEN SAFE_CAST(JSON_VALUE(accuracies, '$.black') AS FLOAT64)
            ELSE NULL
        END AS player_accuracy,
        CASE
            WHEN LOWER(JSON_VALUE(white, '$.username')) = 'peterthiel' THEN SAFE_CAST(JSON_VALUE(accuracies, '$.black') AS FLOAT64)
            WHEN LOWER(JSON_VALUE(black, '$.username')) = 'peterthiel' THEN SAFE_CAST(JSON_VALUE(accuracies, '$.white') AS FLOAT64)
            ELSE NULL
        END AS opponent_accuracy,
        _ingestr_loaded_at AS source_loaded_at
    FROM deduped
    WHERE DATE(end_time, 'UTC') >= DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 10 YEAR)
),

with_outcomes AS (
    SELECT
        *,
        CASE
            WHEN player_result = 'win' THEN 'win'
            WHEN player_result IN ('repetition', 'insufficient', 'timevsinsufficient', 'agreed', 'stalemate', '50move') THEN 'draw'
            WHEN player_result IS NOT NULL THEN 'loss'
            ELSE NULL
        END AS outcome,
        CASE
            WHEN player_result = 'win' THEN 1.0
            WHEN player_result IN ('repetition', 'insufficient', 'timevsinsufficient', 'agreed', 'stalemate', '50move') THEN 0.5
            WHEN player_result IS NOT NULL THEN 0.0
            ELSE NULL
        END AS actual_score,
        LAG(player_rating_after) OVER (
            PARTITION BY time_control
            ORDER BY game_end_time_utc, game_uuid
        ) AS player_rating_before
    FROM normalized
    WHERE player_color IS NOT NULL
),

peer_thresholds AS (
    SELECT
        time_control,
        APPROX_QUANTILES(opponent_rating_after, 100)[SAFE_OFFSET(90)] AS top_opponent_rating_p90
    FROM with_outcomes
    GROUP BY time_control
),

with_peer_thresholds AS (
    SELECT
        games.*,
        thresholds.top_opponent_rating_p90
    FROM with_outcomes AS games
    LEFT JOIN peer_thresholds AS thresholds USING (time_control)
)

SELECT
    game_url,
    game_uuid,
    game_end_time_utc,
    game_start_time_utc,
    CASE
        WHEN game_start_time_utc IS NOT NULL
            AND TIMESTAMP_DIFF(game_end_time_utc, game_start_time_utc, SECOND) >= 0
        THEN TIMESTAMP_DIFF(game_end_time_utc, game_start_time_utc, SECOND)
        ELSE NULL
    END AS game_duration_seconds,
    game_date_utc,
    game_year,
    game_month,
    game_hour_utc,
    day_of_week_utc,
    day_name_utc,
    player_color,
    opponent_username,
    time_class,
    time_control,
    base_time_seconds,
    increment_seconds,
    rated,
    rules,
    player_result,
    outcome,
    actual_score,
    termination,
    player_rating_after,
    player_rating_before,
    opponent_rating_after,
    player_rating_after - opponent_rating_after AS postgame_rating_gap,
    player_rating_before - opponent_rating_after AS rating_gap_proxy,
    CASE
        WHEN player_rating_before IS NOT NULL AND opponent_rating_after IS NOT NULL
        THEN 1.0 / (1.0 + POW(10.0, -(player_rating_before - opponent_rating_after) / 400.0))
        ELSE NULL
    END AS expected_score_proxy,
    CASE
        WHEN player_rating_before IS NOT NULL AND opponent_rating_after IS NOT NULL
        THEN actual_score - (1.0 / (1.0 + POW(10.0, -(player_rating_before - opponent_rating_after) / 400.0)))
        ELSE NULL
    END AS performance_residual_proxy,
    ABS(player_rating_after - opponent_rating_after) <= 100 AS is_postgame_close_rating_peer,
    top_opponent_rating_p90,
    opponent_rating_after >= top_opponent_rating_p90 AS is_top_rated_opponent_encounter,
    ABS(player_rating_after - opponent_rating_after) <= 100
        AND opponent_rating_after >= top_opponent_rating_p90 AS is_top_rated_close_peer,
    opening_eco,
    opening_name,
    has_pgn,
    has_clock_annotations,
    player_accuracy,
    opponent_accuracy,
    player_accuracy IS NOT NULL AS has_accuracy,
    source_loaded_at
FROM with_peer_thresholds
ORDER BY game_end_time_utc, game_uuid
