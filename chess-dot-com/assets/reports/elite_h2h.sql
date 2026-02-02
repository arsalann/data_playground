-- Elite vs Elite Head-to-Head Records
WITH players AS (
    SELECT unnest(ARRAY[
        'francisbegbie', 'Castlecard', 'JolinTsai', 'SeanWinshand', 'GutovAndrey',
        'ManuDavid2910', 'Hikaru', 'ArkadiiKhromaev', 'Philippians46', 'nihalsarin',
        'MagnusCarlsen', 'FabianoCaruana', 'IanNepomniachtchi', 'AnishGiri', 'alireza2003',
        'LevonAronian', 'GothamChess', 'AlexandraBotez', 'EricRosen', 'Firouzja2003'
    ]) as player
)
SELECT 
    LEAST(white_username, black_username) as player1,
    GREATEST(white_username, black_username) as player2,
    SUM(CASE WHEN winner = LEAST(white_username, black_username) THEN 1 ELSE 0 END) as p1_wins,
    SUM(CASE WHEN winner = GREATEST(white_username, black_username) THEN 1 ELSE 0 END) as p2_wins,
    SUM(CASE WHEN winner IS NULL THEN 1 ELSE 0 END) as draws,
    COUNT(*) as total_games,
    ROUND(100.0 * SUM(CASE WHEN winner = LEAST(white_username, black_username) THEN 1 ELSE 0 END) / 
        NULLIF(SUM(CASE WHEN winner IS NOT NULL THEN 1 ELSE 0 END), 0), 1) as p1_win_pct
FROM my_db.staging.games_enriched
WHERE white_username IN (SELECT player FROM players)
  AND black_username IN (SELECT player FROM players)
GROUP BY 1, 2
HAVING COUNT(*) >= 5
ORDER BY total_games DESC
