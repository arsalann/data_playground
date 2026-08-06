/* @bruin
name: chess_peterthiel.report_player_overview
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Produces one evidence-ready summary row for the available public Chess.com
  archive. It combines archive coverage, performance, rating, close-peer,
  session, streak, and gap metrics used by the overview DAC report.

  All dates and habit metrics use public Chess.com timestamps in UTC. The
  account handle is analyzed as a public account and is not used to verify a
  real-world identity.

depends:
  - chess_peterthiel.raw_peterthiel_archives
  - chess_peterthiel.stage_games_enriched
  - chess_peterthiel.stage_game_move_features
  - chess_peterthiel.stage_player_sessions
  - chess_peterthiel.stage_player_activity_streaks

materialization:
  type: table
  strategy: create+replace

columns:
  - name: account_username
    type: VARCHAR
    description: Public Chess.com account handle represented by this summary row.
    primary_key: true
    nullable: false
  - name: available_history_start_utc
    type: TIMESTAMP
    description: Earliest public game-end timestamp available in the loaded archive.
  - name: available_history_end_utc
    type: TIMESTAMP
    description: Latest public game-end timestamp available in the loaded archive.
  - name: available_history_days
    type: INTEGER
    description: Calendar-day span between the earliest and latest available public game dates.
  - name: archive_months
    type: INTEGER
    description: Number of public monthly archive URLs returned by Chess.com.
  - name: games
    type: INTEGER
    description: Number of analyzed public games in the rolling ten-year-or-available-history window.
  - name: wins
    type: INTEGER
    description: Number of target-player wins in the analyzed archive.
  - name: draws
    type: INTEGER
    description: Number of target-player draws in the analyzed archive.
  - name: losses
    type: INTEGER
    description: Number of target-player losses in the analyzed archive.
  - name: score_rate_pct
    type: DOUBLE
    description: Percentage of possible conventional game score earned across analyzed games.
  - name: active_utc_dates
    type: INTEGER
    description: Number of UTC calendar dates with at least one public game.
  - name: peak_rating_after
    type: INTEGER
    description: Highest Chess.com post-game rating snapshot observed in the archive.
  - name: latest_rating_after
    type: INTEGER
    description: Rating snapshot after the latest available public game.
  - name: dominant_time_control
    type: VARCHAR
    description: Most frequently recorded Chess.com time-control string in the archive.
  - name: dominant_time_control_games
    type: INTEGER
    description: Number of games in the most frequent time control.
  - name: dominant_time_control_pct
    type: DOUBLE
    description: Percentage of archive games in the most frequent time control.
  - name: close_rating_peer_games
    type: INTEGER
    description: Games whose public post-game ratings differ by at most 100 points.
  - name: close_rating_peer_score_rate_pct
    type: DOUBLE
    description: Percentage of possible score earned in post-game close-rating peer games.
  - name: top_rated_close_peer_games
    type: INTEGER
    description: Close-rating games against opponents in the top decile of ratings encountered within time control.
  - name: top_rated_close_peer_score_rate_pct
    type: DOUBLE
    description: Percentage of possible score earned in top-decile encountered close-rating peer games.
  - name: longest_active_day_streak
    type: INTEGER
    description: Longest run of consecutive UTC calendar dates with at least one public game.
  - name: busiest_utc_date
    type: DATE
    description: UTC date with the most public games in the archive.
  - name: busiest_utc_date_games
    type: INTEGER
    description: Number of public games on the busiest UTC date.
  - name: longest_session_games
    type: INTEGER
    description: Largest count of games in a session defined by gaps no greater than 30 minutes.
  - name: longest_duration_session_minutes
    type: DOUBLE
    description: Longest elapsed session duration in minutes under the 30-minute gap definition.
  - name: longest_exact_gap_days
    type: DOUBLE
    description: Longest elapsed gap in days between consecutive recorded public games.
  - name: pgn_parsed_game_pct
    type: DOUBLE
    description: Percentage of analyzed games with at least one parsed target-player move.

@bruin */

WITH games AS (
    SELECT *
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched`
),

game_summary AS (
    SELECT
        COUNT(*) AS games,
        COUNTIF(outcome = 'win') AS wins,
        COUNTIF(outcome = 'draw') AS draws,
        COUNTIF(outcome = 'loss') AS losses,
        100.0 * AVG(actual_score) AS score_rate_pct,
        COUNT(DISTINCT game_date_utc) AS active_utc_dates,
        MIN(game_end_time_utc) AS available_history_start_utc,
        MAX(game_end_time_utc) AS available_history_end_utc,
        MAX(player_rating_after) AS peak_rating_after,
        COUNTIF(is_postgame_close_rating_peer) AS close_rating_peer_games,
        100.0 * AVG(IF(is_postgame_close_rating_peer, actual_score, NULL)) AS close_rating_peer_score_rate_pct,
        COUNTIF(is_top_rated_close_peer) AS top_rated_close_peer_games,
        100.0 * AVG(IF(is_top_rated_close_peer, actual_score, NULL)) AS top_rated_close_peer_score_rate_pct
    FROM games
),

latest_game AS (
    SELECT player_rating_after AS latest_rating_after
    FROM games
    QUALIFY ROW_NUMBER() OVER (ORDER BY game_end_time_utc DESC, game_uuid DESC) = 1
),

dominant_format AS (
    SELECT
        time_control AS dominant_time_control,
        COUNT(*) AS dominant_time_control_games
    FROM games
    GROUP BY time_control
    QUALIFY ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, time_control) = 1
),

longest_streak AS (
    SELECT consecutive_active_days AS longest_active_day_streak
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_player_activity_streaks`
    QUALIFY ROW_NUMBER() OVER (ORDER BY consecutive_active_days DESC, streak_start_date_utc) = 1
),

busiest_day AS (
    SELECT game_date_utc AS busiest_utc_date, COUNT(*) AS busiest_utc_date_games
    FROM games
    GROUP BY game_date_utc
    QUALIFY ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, game_date_utc) = 1
),

largest_game_count_session AS (
    SELECT games AS longest_session_games
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_player_sessions`
    QUALIFY ROW_NUMBER() OVER (ORDER BY games DESC, session_duration_seconds DESC) = 1
),

longest_duration_session AS (
    SELECT session_duration_seconds / 60.0 AS longest_duration_session_minutes
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_player_sessions`
    QUALIFY ROW_NUMBER() OVER (ORDER BY session_duration_seconds DESC, games DESC) = 1
),

longest_gap AS (
    SELECT gap_before_seconds / 86400.0 AS longest_exact_gap_days
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_player_sessions`
    WHERE gap_before_seconds IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (ORDER BY gap_before_seconds DESC) = 1
),

move_parse_coverage AS (
    SELECT 100.0 * AVG(CASE WHEN has_parsed_moves THEN 1 ELSE 0 END) AS pgn_parsed_game_pct
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_game_move_features`
),

archive_coverage AS (
    SELECT COUNT(*) AS archive_months
    FROM `bruin-playground-arsalan.chess_peterthiel.raw_peterthiel_archives`
)

SELECT
    'peterthiel' AS account_username,
    summary.available_history_start_utc,
    summary.available_history_end_utc,
    DATE_DIFF(
        DATE(summary.available_history_end_utc, 'UTC'),
        DATE(summary.available_history_start_utc, 'UTC'),
        DAY
    ) + 1 AS available_history_days,
    archives.archive_months,
    summary.games,
    summary.wins,
    summary.draws,
    summary.losses,
    summary.score_rate_pct,
    summary.active_utc_dates,
    summary.peak_rating_after,
    latest.latest_rating_after,
    format.dominant_time_control,
    format.dominant_time_control_games,
    100.0 * SAFE_DIVIDE(format.dominant_time_control_games, summary.games) AS dominant_time_control_pct,
    summary.close_rating_peer_games,
    summary.close_rating_peer_score_rate_pct,
    summary.top_rated_close_peer_games,
    summary.top_rated_close_peer_score_rate_pct,
    streak.longest_active_day_streak,
    busiest.busiest_utc_date,
    busiest.busiest_utc_date_games,
    session_games.longest_session_games,
    session_duration.longest_duration_session_minutes,
    gap.longest_exact_gap_days,
    moves.pgn_parsed_game_pct
FROM game_summary AS summary
CROSS JOIN latest_game AS latest
CROSS JOIN dominant_format AS format
CROSS JOIN longest_streak AS streak
CROSS JOIN busiest_day AS busiest
CROSS JOIN largest_game_count_session AS session_games
CROSS JOIN longest_duration_session AS session_duration
CROSS JOIN longest_gap AS gap
CROSS JOIN move_parse_coverage AS moves
CROSS JOIN archive_coverage AS archives
