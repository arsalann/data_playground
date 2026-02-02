-- Top Players Statistics
WITH players AS (
    SELECT unnest(ARRAY[
        'francisbegbie', 'Castlecard', 'JolinTsai', 'SeanWinshand', 'GutovAndrey',
        'ManuDavid2910', 'Hikaru', 'ArkadiiKhromaev', 'Philippians46', 'nihalsarin',
        'MagnusCarlsen', 'FabianoCaruana', 'IanNepomniachtchi', 'AnishGiri', 'alireza2003',
        'LevonAronian', 'GothamChess', 'AlexandraBotez', 'EricRosen', 'Firouzja2003'
    ]) as player
),
player_games AS (
    SELECT 
        white_username as player,
        CASE WHEN winner = white_username THEN 1 ELSE 0 END as won,
        CASE WHEN winner IS NOT NULL AND winner != white_username THEN 1 ELSE 0 END as lost,
        CASE WHEN winner IS NULL THEN 1 ELSE 0 END as drew,
        time_class,
        white_rating as rating,
        'white' as color
    FROM my_db.staging.games_enriched
    WHERE white_username IN (SELECT player FROM players)
    
    UNION ALL
    
    SELECT 
        black_username as player,
        CASE WHEN winner = black_username THEN 1 ELSE 0 END as won,
        CASE WHEN winner IS NOT NULL AND winner != black_username THEN 1 ELSE 0 END as lost,
        CASE WHEN winner IS NULL THEN 1 ELSE 0 END as drew,
        time_class,
        black_rating as rating,
        'black' as color
    FROM my_db.staging.games_enriched
    WHERE black_username IN (SELECT player FROM players)
)
SELECT 
    player,
    COUNT(*) as total_games,
    SUM(won) as wins,
    SUM(lost) as losses,
    SUM(drew) as draws,
    ROUND(100.0 * SUM(won) / COUNT(*), 2) as win_rate,
    ROUND(AVG(rating), 0) as avg_rating,
    MAX(rating) as peak_rating,
    SUM(CASE WHEN time_class = 'bullet' THEN 1 ELSE 0 END) as bullet_games,
    SUM(CASE WHEN time_class = 'blitz' THEN 1 ELSE 0 END) as blitz_games,
    SUM(CASE WHEN time_class = 'rapid' THEN 1 ELSE 0 END) as rapid_games,
    ROUND(100.0 * SUM(CASE WHEN color = 'white' AND won = 1 THEN 1 ELSE 0 END) / 
        NULLIF(SUM(CASE WHEN color = 'white' THEN 1 ELSE 0 END), 0), 2) as white_win_rate,
    ROUND(100.0 * SUM(CASE WHEN color = 'black' AND won = 1 THEN 1 ELSE 0 END) / 
        NULLIF(SUM(CASE WHEN color = 'black' THEN 1 ELSE 0 END), 0), 2) as black_win_rate
FROM player_games
GROUP BY player
ORDER BY win_rate DESC
