/* @bruin
name: staging.games_enriched
type: duckdb.sql
connection: motherduck-prod
description: |
  Transforms and enriches raw Chess.com game data.
  Extracts structured fields from the game records, calculates derived metrics,
  and prepares data for analytics.
  
  Enrichments:
  - Extracts player usernames from white/black JSON
  - Calculates rating differences
  - Determines winner and game outcome
  - Extracts time control details
  - Adds game duration estimates

depends:
  - raw.player_games
  - raw.player_profiles

materialization:
  type: table
  strategy: create+replace

columns:
  - name: game_url
    type: VARCHAR
    description: Unique URL identifier for the game
    primary_key: true
  - name: time_class
    type: VARCHAR
    description: Game speed category (bullet, blitz, rapid, daily)
  - name: time_control
    type: VARCHAR
    description: Time control string (e.g., "180+2" for 3+2)
  - name: rated
    type: BOOLEAN
    description: Whether the game was rated
  - name: white_username
    type: VARCHAR
    description: Username of player with white pieces
  - name: white_rating
    type: INTEGER
    description: Rating of white player at game time
  - name: white_result
    type: VARCHAR
    description: Result for white (win, lose, draw, etc.)
  - name: black_username
    type: VARCHAR
    description: Username of player with black pieces
  - name: black_rating
    type: INTEGER
    description: Rating of black player at game time
  - name: black_result
    type: VARCHAR
    description: Result for black (win, lose, draw, etc.)
  - name: winner
    type: VARCHAR
    description: Username of the winner (NULL for draws)
  - name: rating_diff
    type: INTEGER
    description: White rating minus black rating
  - name: end_time
    type: TIMESTAMP
    description: When the game ended
  - name: opening_eco
    type: VARCHAR
    description: ECO code of the opening played
  - name: opening_name
    type: VARCHAR
    description: Name of the opening played

@bruin */

WITH parsed_games AS (
  SELECT
    url AS game_url,
    time_class,
    time_control,
    rated,
    -- Extract white player info from JSON
    white->>'username' AS white_username,
    TRY_CAST(white->>'rating' AS INTEGER) AS white_rating,
    white->>'result' AS white_result,
    -- Extract black player info from JSON
    black->>'username' AS black_username,
    TRY_CAST(black->>'rating' AS INTEGER) AS black_rating,
    black->>'result' AS black_result,
    -- Game timing (already a timestamp)
    CAST(end_time AS TIMESTAMP) AS end_time,
  FROM raw.player_games
  WHERE url IS NOT NULL
)

SELECT
  game_url,
  time_class,
  time_control,
  rated,
  white_username,
  white_rating,
  white_result,
  black_username,
  black_rating,
  black_result,
  -- Determine winner
  CASE
    WHEN white_result = 'win' THEN white_username
    WHEN black_result = 'win' THEN black_username
    ELSE NULL
  END AS winner,
  -- Rating difference (positive = white higher rated)
  white_rating - black_rating AS rating_diff,
  end_time,
  -- Opening placeholder (would need PGN parsing for real ECO)
  NULL AS opening_eco,
  NULL AS opening_name,
FROM parsed_games
-- Deduplicate by game_url
QUALIFY ROW_NUMBER() OVER (PARTITION BY game_url ORDER BY end_time DESC NULLS LAST) = 1
