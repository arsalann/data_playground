-- Biggest Upsets: Lower-rated players beating higher-rated players
WITH players AS (
    SELECT unnest(ARRAY[
        'francisbegbie', 'Castlecard', 'JolinTsai', 'SeanWinshand', 'GutovAndrey',
        'ManuDavid2910', 'Hikaru', 'ArkadiiKhromaev', 'Philippians46', 'nihalsarin',
        'MagnusCarlsen', 'FabianoCaruana', 'IanNepomniachtchi', 'AnishGiri', 'alireza2003',
        'LevonAronian', 'GothamChess', 'AlexandraBotez', 'EricRosen', 'Firouzja2003'
    ]) as player
)
SELECT 
    winner,
    CASE WHEN winner = white_username THEN black_username ELSE white_username END as loser,
    CASE WHEN winner = white_username THEN white_rating ELSE black_rating END as winner_rating,
    CASE WHEN winner = white_username THEN black_rating ELSE white_rating END as loser_rating,
    ABS(white_rating - black_rating) as rating_gap,
    time_class,
    game_url
FROM my_db.staging.games_enriched
WHERE winner IS NOT NULL
  AND (white_username IN (SELECT player FROM players) OR black_username IN (SELECT player FROM players))
  AND (
      (winner = white_username AND white_rating < black_rating - 500)
      OR 
      (winner = black_username AND black_rating < white_rating - 500)
  )
ORDER BY rating_gap DESC
LIMIT 20
