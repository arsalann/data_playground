/* @bruin
name: staging.player_stats
type: duckdb.sql
connection: motherduck-prod
description: |
  Calculates aggregate statistics for each player.
  Combines profile data with game statistics to create a comprehensive player summary.
  
  Metrics calculated:
  - Total games played (as white and black)
  - Win/loss/draw counts and percentages
  - Performance by time class
  - Average opponent rating
  - Win rate when higher/lower rated

depends:
  - staging.games_enriched
  - raw.player_profiles

materialization:
  type: table
  strategy: create+replace

columns:
  - name: username
    type: VARCHAR
    description: Player's Chess.com username
    primary_key: true
  - name: name
    type: VARCHAR
    description: Player's display name
  - name: country
    type: VARCHAR
    description: Player's country code
  - name: followers
    type: INTEGER
    description: Number of followers
  - name: total_games
    type: INTEGER
    description: Total number of games played
  - name: games_as_white
    type: INTEGER
    description: Games played with white pieces
  - name: games_as_black
    type: INTEGER
    description: Games played with black pieces
  - name: wins
    type: INTEGER
    description: Total wins
  - name: losses
    type: INTEGER
    description: Total losses
  - name: draws
    type: INTEGER
    description: Total draws
  - name: win_rate
    type: DOUBLE
    description: Win percentage (0-100)
  - name: win_rate_as_white
    type: DOUBLE
    description: Win percentage when playing white
  - name: win_rate_as_black
    type: DOUBLE
    description: Win percentage when playing black

@bruin */

WITH white_games AS (
  SELECT
    white_username AS username,
    COUNT(*) AS games,
    SUM(CASE WHEN white_result = 'win' THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN white_result IN ('checkmated', 'resigned', 'timeout', 'abandoned') THEN 1 ELSE 0 END) AS losses,
    SUM(CASE WHEN white_result IN ('stalemate', 'agreed', 'repetition', 'insufficient', '50move', 'timevsinsufficient') THEN 1 ELSE 0 END) AS draws,
    AVG(black_rating) AS avg_opponent_rating,
  FROM staging.games_enriched
  GROUP BY white_username
),

black_games AS (
  SELECT
    black_username AS username,
    COUNT(*) AS games,
    SUM(CASE WHEN black_result = 'win' THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN black_result IN ('checkmated', 'resigned', 'timeout', 'abandoned') THEN 1 ELSE 0 END) AS losses,
    SUM(CASE WHEN black_result IN ('stalemate', 'agreed', 'repetition', 'insufficient', '50move', 'timevsinsufficient') THEN 1 ELSE 0 END) AS draws,
    AVG(white_rating) AS avg_opponent_rating,
  FROM staging.games_enriched
  GROUP BY black_username
),

combined_stats AS (
  SELECT
    COALESCE(w.username, b.username) AS username,
    COALESCE(w.games, 0) AS games_as_white,
    COALESCE(b.games, 0) AS games_as_black,
    COALESCE(w.games, 0) + COALESCE(b.games, 0) AS total_games,
    COALESCE(w.wins, 0) + COALESCE(b.wins, 0) AS wins,
    COALESCE(w.losses, 0) + COALESCE(b.losses, 0) AS losses,
    COALESCE(w.draws, 0) + COALESCE(b.draws, 0) AS draws,
    w.wins AS white_wins,
    w.games AS white_total,
    b.wins AS black_wins,
    b.games AS black_total,
  FROM white_games w
  FULL OUTER JOIN black_games b ON w.username = b.username
)

SELECT
  cs.username,
  p.name,
  p.country,
  TRY_CAST(p.followers AS INTEGER) AS followers,
  cs.total_games,
  cs.games_as_white,
  cs.games_as_black,
  cs.wins,
  cs.losses,
  cs.draws,
  ROUND(100.0 * cs.wins / NULLIF(cs.total_games, 0), 2) AS win_rate,
  ROUND(100.0 * cs.white_wins / NULLIF(cs.white_total, 0), 2) AS win_rate_as_white,
  ROUND(100.0 * cs.black_wins / NULLIF(cs.black_total, 0), 2) AS win_rate_as_black,
FROM combined_stats cs
LEFT JOIN raw.player_profiles p ON LOWER(cs.username) = LOWER(p.username)
WHERE cs.total_games > 0
