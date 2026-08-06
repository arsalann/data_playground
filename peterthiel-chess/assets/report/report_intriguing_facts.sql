/* @bruin
name: chess_peterthiel.report_intriguing_facts
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Curates a small, reproducible set of striking archive facts for the DAC
  report. Each fact remains tied to an explicit calculation and caveat instead
  of presenting a dramatic claim without evidence.

depends:
  - chess_peterthiel.report_player_overview
  - chess_peterthiel.report_peer_competition
  - chess_peterthiel.report_first_move_repertoire
  - chess_peterthiel.report_clock_outcomes
  - chess_peterthiel.report_opponent_matchups

materialization:
  type: table
  strategy: create+replace

columns:
  - name: fact_key
    type: VARCHAR
    description: Stable identifier for the evidence-backed archive fact.
    primary_key: true
    nullable: false
  - name: fact_order
    type: INTEGER
    description: Display order for the fact table.
  - name: fact_label
    type: VARCHAR
    description: Concise statement of the measured public-account fact.
  - name: value_numeric
    type: DOUBLE
    description: Primary numeric value supporting the fact.
  - name: value_unit
    type: VARCHAR
    description: Unit for the primary numeric value.
  - name: evidence
    type: VARCHAR
    description: Sample size or comparison that makes the fact auditable.
  - name: caveat
    type: VARCHAR
    description: Fact-specific interpretation limit.

@bruin */

WITH overview AS (
    SELECT *
    FROM `bruin-playground-arsalan.chess_peterthiel.report_player_overview`
),

white_e4 AS (
    SELECT *
    FROM `bruin-playground-arsalan.chess_peterthiel.report_first_move_repertoire`
    WHERE player_color = 'white' AND first_move = 'e4'
),

top_peers AS (
    SELECT *
    FROM `bruin-playground-arsalan.chess_peterthiel.report_peer_competition`
    WHERE cohort_key = 'top_rated_close_peers'
),

clock_comparison AS (
    SELECT
        MAX(IF(min_clock_bucket = 'Under 5 seconds', score_rate_pct, NULL)) AS under_5_score_rate_pct,
        MAX(IF(min_clock_bucket = '30 or more seconds', score_rate_pct, NULL)) AS over_30_score_rate_pct,
        MAX(IF(min_clock_bucket = 'Under 5 seconds', games, NULL)) AS under_5_games,
        MAX(IF(min_clock_bucket = '30 or more seconds', games, NULL)) AS over_30_games
    FROM `bruin-playground-arsalan.chess_peterthiel.report_clock_outcomes`
),

opponent_variety AS (
    SELECT
        COUNT(*) AS unique_opponents,
        100.0 * AVG(CASE WHEN games = 1 THEN 1 ELSE 0 END) AS one_game_opponent_pct,
        MAX(games) AS maximum_games_against_one_opponent
    FROM `bruin-playground-arsalan.chess_peterthiel.report_opponent_matchups`
)

SELECT
    'format_concentration' AS fact_key,
    1 AS fact_order,
    'Nearly every public game is three-minute blitz' AS fact_label,
    dominant_time_control_pct AS value_numeric,
    'percent of games' AS value_unit,
    CONCAT(CAST(dominant_time_control_games AS STRING), ' of ', CAST(games AS STRING), ' games use ', dominant_time_control, '.') AS evidence,
    'This describes the available public archive, not all chess played by a person.' AS caveat
FROM overview

UNION ALL

SELECT
    'white_e4_concentration' AS fact_key,
    2 AS fact_order,
    'The public account opens as White with e4 almost exclusively' AS fact_label,
    usage_pct AS value_numeric,
    'percent of White games' AS value_unit,
    CONCAT(CAST(games AS STRING), ' games; score rate ', CAST(ROUND(score_rate_pct, 1) AS STRING), '%.') AS evidence,
    'First-move concentration is a repertoire signal, not an engine assessment of the opening.' AS caveat
FROM white_e4

UNION ALL

SELECT
    'top_close_peer_score' AS fact_key,
    3 AS fact_order,
    'Score against top-rated close peers encountered is below the rating proxy' AS fact_label,
    actual_score_pct - expected_score_proxy_pct AS value_numeric,
    'percentage-point residual' AS value_unit,
    CONCAT(CAST(games AS STRING), ' games; actual ', CAST(ROUND(actual_score_pct, 1) AS STRING), '% vs proxy ', CAST(ROUND(expected_score_proxy_pct, 1) AS STRING), '%.') AS evidence,
    'Top means the top decile of opponents encountered, not a historical platform-wide ranking; opponent ratings are post-game snapshots.' AS caveat
FROM top_peers

UNION ALL

SELECT
    'clock_pressure_association' AS fact_key,
    4 AS fact_order,
    'Games that ever fall below five seconds show a much lower score rate' AS fact_label,
    under_5_score_rate_pct - over_30_score_rate_pct AS value_numeric,
    'percentage-point score-rate difference' AS value_unit,
    CONCAT(CAST(under_5_games AS STRING), ' under-five-second games scored ', CAST(ROUND(under_5_score_rate_pct, 1) AS STRING), '%; ', CAST(over_30_games AS STRING), ' games at 30+ seconds scored ', CAST(ROUND(over_30_score_rate_pct, 1) AS STRING), '%.') AS evidence,
    'Minimum clock is observed within the same game as the outcome, so this is association rather than evidence of causation.' AS caveat
FROM clock_comparison

UNION ALL

SELECT
    'opponent_variety' AS fact_key,
    5 AS fact_order,
    'The public archive is exceptionally opponent-diverse' AS fact_label,
    one_game_opponent_pct AS value_numeric,
    'percent of opponents faced once' AS value_unit,
    CONCAT(CAST(unique_opponents AS STRING), ' unique opponents; no opponent appears more than ', CAST(maximum_games_against_one_opponent AS STRING), ' times.') AS evidence,
    'Opponent names and repetition counts reflect only public games retained by Chess.com.' AS caveat
FROM opponent_variety

UNION ALL

SELECT
    'activity_streak' AS fact_key,
    6 AS fact_order,
    'The longest public active-day streak spans more than three months' AS fact_label,
    CAST(longest_active_day_streak AS FLOAT64) AS value_numeric,
    'consecutive UTC active days' AS value_unit,
    CONCAT(CAST(longest_active_day_streak AS STRING), ' consecutive UTC dates; busiest date has ', CAST(busiest_utc_date_games AS STRING), ' games.') AS evidence,
    'An active day means one or more archived games in UTC; it does not measure continuous play or local-time habits.' AS caveat
FROM overview
