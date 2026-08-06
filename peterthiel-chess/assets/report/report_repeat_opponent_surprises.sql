/* @bruin
name: chess_peterthiel.report_repeat_opponent_surprises
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Identifies the 15 repeat public 3+0 matchups with the largest shortfall
  versus the documented rating-derived expected-score proxy, after requiring
  at least eight games and an average expected score of at least 35 percent.
  The filter reduces trivial cases against much stronger opponents but does
  not turn the table into a causal or independent statistical experiment.

depends:
  - chess_peterthiel.stage_games_enriched

materialization:
  type: table
  strategy: create+replace

columns:
  - name: matchup_rank
    type: INTEGER
    description: Rank among eligible repeat matchups by most negative performance residual proxy.
    primary_key: true
    nullable: false
  - name: opponent_username
    type: VARCHAR
    description: Public Chess.com opponent username as recorded in archived games.
    nullable: false
  - name: games
    type: INTEGER
    description: Number of eligible dominant-time-control public games in the matchup.
  - name: wins
    type: INTEGER
    description: Number of target-player wins in the matchup.
  - name: draws
    type: INTEGER
    description: Number of target-player draws in the matchup.
  - name: losses
    type: INTEGER
    description: Number of target-player losses in the matchup.
  - name: actual_score_pct
    type: DOUBLE
    description: Conventional public game score as a percentage of possible points.
  - name: expected_score_proxy_pct
    type: DOUBLE
    description: Mean rating-derived expected-score proxy in percent.
  - name: performance_residual_proxy_pp
    type: DOUBLE
    description: Actual score minus expected-score proxy in percentage points.
  - name: average_opponent_rating_after
    type: DOUBLE
    description: Mean opponent public post-game rating snapshot in the matchup.
  - name: peak_opponent_rating_after
    type: INTEGER
    description: Highest opponent public post-game rating snapshot observed in the matchup.
  - name: first_game_utc
    type: DATE
    description: UTC date of the first archived game in the matchup.
  - name: last_game_utc
    type: DATE
    description: UTC date of the last archived game in the matchup.
  - name: distinct_utc_dates
    type: INTEGER
    description: Count of distinct UTC dates represented by the matchup.

@bruin */

WITH dominant_time_control AS (
    SELECT time_control
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched`
    GROUP BY time_control
    QUALIFY ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, time_control) = 1
),

matchups AS (
    SELECT
        opponent_username,
        COUNT(*) AS games,
        COUNTIF(outcome = 'win') AS wins,
        COUNTIF(outcome = 'draw') AS draws,
        COUNTIF(outcome = 'loss') AS losses,
        100.0 * AVG(actual_score) AS actual_score_pct,
        100.0 * AVG(expected_score_proxy) AS expected_score_proxy_pct,
        100.0 * AVG(performance_residual_proxy) AS performance_residual_proxy_pp,
        AVG(opponent_rating_after) AS average_opponent_rating_after,
        MAX(opponent_rating_after) AS peak_opponent_rating_after,
        MIN(game_date_utc) AS first_game_utc,
        MAX(game_date_utc) AS last_game_utc,
        COUNT(DISTINCT game_date_utc) AS distinct_utc_dates
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched`
    INNER JOIN dominant_time_control USING (time_control)
    GROUP BY opponent_username
)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY performance_residual_proxy_pp, games DESC, opponent_username
    ) AS matchup_rank,
    *
FROM matchups
WHERE games >= 8
    AND expected_score_proxy_pct >= 35
QUALIFY matchup_rank <= 15
ORDER BY matchup_rank
