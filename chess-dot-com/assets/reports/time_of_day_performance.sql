-- Time of day performance: When do players perform best?
WITH players AS (
    SELECT unnest(ARRAY[
        'francisbegbie', 'Castlecard', 'JolinTsai', 'SeanWinshand', 'GutovAndrey',
        'ManuDavid2910', 'Hikaru', 'ArkadiiKhromaev', 'Philippians46', 'nihalsarin',
        'MagnusCarlsen', 'FabianoCaruana', 'IanNepomniachtchi', 'AnishGiri', 'alireza2003',
        'LevonAronian', 'GothamChess', 'AlexandraBotez', 'EricRosen', 'Firouzja2003'
    ]) as player
),
player_hourly AS (
    SELECT 
        white_username as player,
        EXTRACT(hour FROM end_time) as hour,
        CASE WHEN winner = white_username THEN 1 ELSE 0 END as won,
        CASE WHEN winner IS NOT NULL THEN 1 ELSE 0 END as decisive
    FROM my_db.staging.games_enriched
    WHERE white_username IN (SELECT player FROM players)
    
    UNION ALL
    
    SELECT 
        black_username as player,
        EXTRACT(hour FROM end_time) as hour,
        CASE WHEN winner = black_username THEN 1 ELSE 0 END as won,
        CASE WHEN winner IS NOT NULL THEN 1 ELSE 0 END as decisive
    FROM my_db.staging.games_enriched
    WHERE black_username IN (SELECT player FROM players)
)
SELECT 
    player,
    hour,
    COUNT(*) as games,
    ROUND(100.0 * SUM(won) / NULLIF(SUM(decisive), 0), 2) as win_rate
FROM player_hourly
GROUP BY player, hour
HAVING COUNT(*) >= 30
ORDER BY player, hour
