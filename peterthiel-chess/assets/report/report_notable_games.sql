/* @bruin
name: chess_peterthiel.report_notable_games
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Surfaces traceable public games that are notable under transparent rules:
  largest underdog wins and favored losses after excluding low-rating opponents,
  plus longest parsed games. Rating-based categories use the documented target
  pre-game and opponent post-game rating proxy rather than an official Elo model.

depends:
  - chess_peterthiel.stage_games_enriched
  - chess_peterthiel.stage_game_move_features

materialization:
  type: table
  strategy: create+replace

columns:
  - name: notable_kind
    type: VARCHAR
    description: Transparent rule used to select the game as notable.
    primary_key: true
    nullable: false
  - name: notable_rank
    type: INTEGER
    description: Rank within the notable-game selection rule.
    primary_key: true
    nullable: false
  - name: game_url
    type: VARCHAR
    description: Public Chess.com URL for independent inspection of the game.
  - name: game_date_utc
    type: DATE
    description: UTC calendar date of the notable game.
  - name: opponent_username
    type: VARCHAR
    description: Opponent public Chess.com username.
  - name: outcome
    type: VARCHAR
    description: Simplified target-player game outcome.
  - name: player_rating_before
    type: INTEGER
    description: Reconstructed target-player pre-game rating proxy.
  - name: opponent_rating_after
    type: INTEGER
    description: Opponent public post-game rating snapshot.
  - name: rating_gap_proxy
    type: INTEGER
    description: Target rating proxy minus opponent post-game rating in rating points.
  - name: expected_score_proxy_pct
    type: DOUBLE
    description: Rating-derived expected-score proxy in percent.
  - name: opening_eco
    type: VARCHAR
    description: ECO code parsed from the public PGN.
  - name: opening_name
    type: VARCHAR
    description: Opening label parsed from Chess.com's public opening URL.
  - name: player_move_count
    type: INTEGER
    description: Parsed target-player move count in the game.
  - name: minimum_clock_seconds_remaining
    type: DOUBLE
    description: Lowest public target-player remaining clock annotation in seconds.

@bruin */

WITH dominant_time_control AS (
    SELECT time_control
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched`
    GROUP BY time_control
    QUALIFY ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, time_control) = 1
),

eligible_games AS (
    SELECT
        games.*,
        features.player_move_count,
        features.minimum_clock_seconds_remaining
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched` AS games
    INNER JOIN `bruin-playground-arsalan.chess_peterthiel.stage_game_move_features` AS features USING (game_url)
    INNER JOIN dominant_time_control
        ON games.time_control = dominant_time_control.time_control
    WHERE games.player_rating_before >= 1800
        AND games.opponent_rating_after >= 1800
)

SELECT
    'Underdog win' AS notable_kind,
    ROW_NUMBER() OVER (ORDER BY rating_gap_proxy, game_end_time_utc) AS notable_rank,
    game_url,
    game_date_utc,
    opponent_username,
    outcome,
    player_rating_before,
    opponent_rating_after,
    rating_gap_proxy,
    100.0 * expected_score_proxy AS expected_score_proxy_pct,
    opening_eco,
    opening_name,
    player_move_count,
    minimum_clock_seconds_remaining
FROM eligible_games
WHERE outcome = 'win'
    AND rating_gap_proxy <= -200
QUALIFY notable_rank <= 10

UNION ALL

SELECT
    'Favored loss' AS notable_kind,
    ROW_NUMBER() OVER (ORDER BY rating_gap_proxy DESC, game_end_time_utc) AS notable_rank,
    game_url,
    game_date_utc,
    opponent_username,
    outcome,
    player_rating_before,
    opponent_rating_after,
    rating_gap_proxy,
    100.0 * expected_score_proxy AS expected_score_proxy_pct,
    opening_eco,
    opening_name,
    player_move_count,
    minimum_clock_seconds_remaining
FROM eligible_games
WHERE outcome = 'loss'
    AND rating_gap_proxy >= 200
QUALIFY notable_rank <= 10

UNION ALL

SELECT
    'Longest parsed game' AS notable_kind,
    ROW_NUMBER() OVER (ORDER BY player_move_count DESC, game_end_time_utc) AS notable_rank,
    game_url,
    game_date_utc,
    opponent_username,
    outcome,
    player_rating_before,
    opponent_rating_after,
    rating_gap_proxy,
    100.0 * expected_score_proxy AS expected_score_proxy_pct,
    opening_eco,
    opening_name,
    player_move_count,
    minimum_clock_seconds_remaining
FROM eligible_games
WHERE player_move_count > 0
QUALIFY notable_rank <= 10
