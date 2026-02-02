-- How players lose: timeout vs resignation vs checkmate
WITH players AS (
    SELECT unnest(ARRAY[
        'francisbegbie', 'Castlecard', 'JolinTsai', 'SeanWinshand', 'GutovAndrey',
        'ManuDavid2910', 'Hikaru', 'ArkadiiKhromaev', 'Philippians46', 'nihalsarin',
        'MagnusCarlsen', 'FabianoCaruana', 'IanNepomniachtchi', 'AnishGiri', 'alireza2003',
        'LevonAronian', 'GothamChess', 'AlexandraBotez', 'EricRosen', 'Firouzja2003'
    ]) as player
),
player_results AS (
    SELECT 
        white_username as player,
        white_result as result,
        winner != white_username AND winner IS NOT NULL as lost
    FROM my_db.staging.games_enriched
    WHERE white_username IN (SELECT player FROM players)
    
    UNION ALL
    
    SELECT 
        black_username as player,
        black_result as result,
        winner != black_username AND winner IS NOT NULL as lost
    FROM my_db.staging.games_enriched
    WHERE black_username IN (SELECT player FROM players)
)
SELECT 
    player,
    SUM(CASE WHEN lost AND result = 'timeout' THEN 1 ELSE 0 END) as lost_on_time,
    SUM(CASE WHEN lost AND result = 'resigned' THEN 1 ELSE 0 END) as resigned,
    SUM(CASE WHEN lost AND result = 'checkmated' THEN 1 ELSE 0 END) as got_checkmated,
    SUM(CASE WHEN lost THEN 1 ELSE 0 END) as total_losses,
    COUNT(*) as total_games,
    ROUND(100.0 * SUM(CASE WHEN lost AND result = 'checkmated' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) as checkmate_rate
FROM player_results
GROUP BY player
ORDER BY checkmate_rate ASC
