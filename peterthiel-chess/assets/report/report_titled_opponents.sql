/* @bruin
name: chess_peterthiel.report_titled_opponents
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Joins dominant-time-control public games to Chess.com's current public
  titled-player registries. It identifies opponents whose usernames currently
  appear in a registry, rather than treating rating or a username string as
  proof of a title.

depends:
  - chess_peterthiel.raw_chess_titled_players
  - chess_peterthiel.stage_games_enriched

materialization:
  type: table
  strategy: create+replace

columns:
  - name: opponent_username
    type: VARCHAR
    description: Archived public Chess.com opponent username matching a current title-registry entry.
    primary_key: true
    nullable: false
  - name: opponent_title
    type: VARCHAR
    description: Current Chess.com public title-registry code selected by title precedence.
    nullable: false
  - name: games
    type: INTEGER
    description: Number of dominant-time-control public games against the titled opponent.
  - name: wins
    type: INTEGER
    description: Number of target-player wins against the titled opponent.
  - name: draws
    type: INTEGER
    description: Number of target-player draws against the titled opponent.
  - name: losses
    type: INTEGER
    description: Number of target-player losses against the titled opponent.
  - name: score_rate_pct
    type: DOUBLE
    description: Conventional public game score as a percentage of possible points.
  - name: peak_opponent_rating_after
    type: INTEGER
    description: Highest opponent public post-game rating snapshot in the matchup.
  - name: first_game_utc
    type: DATE
    description: UTC date of the first archived game against the titled opponent.
  - name: last_game_utc
    type: DATE
    description: UTC date of the last archived game against the titled opponent.

@bruin */

WITH dominant_time_control AS (
    SELECT time_control
    FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched`
    GROUP BY time_control
    QUALIFY ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, time_control) = 1
),

title_precedence AS (
    SELECT
        LOWER(username) AS username_key,
        title,
        ROW_NUMBER() OVER (
            PARTITION BY LOWER(username)
            ORDER BY CASE title
                WHEN 'GM' THEN 1
                WHEN 'IM' THEN 2
                WHEN 'FM' THEN 3
                WHEN 'NM' THEN 4
                WHEN 'WGM' THEN 5
                WHEN 'WIM' THEN 6
                WHEN 'WFM' THEN 7
                WHEN 'WNM' THEN 8
                ELSE 9
            END
        ) AS title_rank
    FROM `bruin-playground-arsalan.chess_peterthiel.raw_chess_titled_players`
)

SELECT
    game_records.opponent_username,
    titles.title AS opponent_title,
    COUNT(*) AS games,
    COUNTIF(game_records.outcome = 'win') AS wins,
    COUNTIF(game_records.outcome = 'draw') AS draws,
    COUNTIF(game_records.outcome = 'loss') AS losses,
    100.0 * AVG(game_records.actual_score) AS score_rate_pct,
    MAX(game_records.opponent_rating_after) AS peak_opponent_rating_after,
    MIN(game_records.game_date_utc) AS first_game_utc,
    MAX(game_records.game_date_utc) AS last_game_utc
FROM `bruin-playground-arsalan.chess_peterthiel.stage_games_enriched` AS game_records
INNER JOIN dominant_time_control USING (time_control)
INNER JOIN title_precedence AS titles
    ON LOWER(game_records.opponent_username) = titles.username_key
    AND titles.title_rank = 1
GROUP BY game_records.opponent_username, titles.title
ORDER BY peak_opponent_rating_after DESC, games DESC, opponent_username
