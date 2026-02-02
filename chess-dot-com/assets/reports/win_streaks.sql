-- Win streaks: Longest consecutive wins for each player
WITH players AS (
    SELECT unnest(ARRAY[
        'francisbegbie', 'Castlecard', 'JolinTsai', 'SeanWinshand', 'GutovAndrey',
        'ManuDavid2910', 'Hikaru', 'ArkadiiKhromaev', 'Philippians46', 'nihalsarin',
        'MagnusCarlsen', 'FabianoCaruana', 'IanNepomniachtchi', 'AnishGiri', 'alireza2003',
        'LevonAronian', 'GothamChess', 'AlexandraBotez', 'EricRosen', 'Firouzja2003'
    ]) as player
),
all_player_games AS (
    SELECT 
        white_username as player,
        end_time,
        CASE WHEN winner = white_username THEN 1 ELSE 0 END as won
    FROM my_db.staging.games_enriched
    WHERE white_username IN (SELECT player FROM players)
    
    UNION ALL
    
    SELECT 
        black_username as player,
        end_time,
        CASE WHEN winner = black_username THEN 1 ELSE 0 END as won
    FROM my_db.staging.games_enriched
    WHERE black_username IN (SELECT player FROM players)
),
ordered AS (
    SELECT 
        player,
        end_time,
        won,
        ROW_NUMBER() OVER (PARTITION BY player ORDER BY end_time) - 
        ROW_NUMBER() OVER (PARTITION BY player, won ORDER BY end_time) as grp
    FROM all_player_games
),
streaks AS (
    SELECT 
        player,
        won,
        COUNT(*) as streak_len
    FROM ordered
    GROUP BY player, won, grp
)
SELECT 
    player,
    MAX(streak_len) as longest_win_streak
FROM streaks
WHERE won = 1
GROUP BY player
ORDER BY longest_win_streak DESC
