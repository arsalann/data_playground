-- Biggest Upsets: when lower-rated players beat higher-rated ones
WITH tracked_players AS (
    SELECT username FROM my_db.raw.player_profiles
)
SELECT
    winner,
    CASE WHEN winner = white_username THEN black_username ELSE white_username END AS loser,
    CASE WHEN winner = white_username THEN white_rating ELSE black_rating END AS winner_rating,
    CASE WHEN winner = white_username THEN black_rating ELSE white_rating END AS loser_rating,
    ABS(white_rating - black_rating) AS rating_gap,
    time_class,
    end_time,
    game_url
FROM my_db.staging.games_enriched
WHERE winner IS NOT NULL
  AND (
    LOWER(white_username) IN (SELECT LOWER(username) FROM tracked_players)
    OR LOWER(black_username) IN (SELECT LOWER(username) FROM tracked_players)
  )
  AND (
      (winner = white_username AND white_rating < black_rating - 100)
      OR
      (winner = black_username AND black_rating < white_rating - 100)
  )
ORDER BY rating_gap DESC
LIMIT 25
