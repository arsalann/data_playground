-- Opening analysis: What openings do players use and win with?
WITH players AS (
    SELECT unnest(ARRAY[
        'francisbegbie', 'Castlecard', 'JolinTsai', 'SeanWinshand', 'GutovAndrey',
        'ManuDavid2910', 'Hikaru', 'ArkadiiKhromaev', 'Philippians46', 'nihalsarin',
        'MagnusCarlsen', 'FabianoCaruana', 'IanNepomniachtchi', 'AnishGiri', 'alireza2003',
        'LevonAronian', 'GothamChess', 'AlexandraBotez', 'EricRosen', 'Firouzja2003'
    ]) as player
),
games_with_opening AS (
    SELECT 
        g.white_username,
        g.black_username,
        g.winner,
        p.eco,
        SPLIT_PART(REPLACE(REGEXP_EXTRACT(p.eco, '/openings/([^?]+)', 1), '-', ' '), ' ', 1) || ' ' ||
        SPLIT_PART(REPLACE(REGEXP_EXTRACT(p.eco, '/openings/([^?]+)', 1), '-', ' '), ' ', 2) as opening_short
    FROM my_db.staging.games_enriched g
    JOIN my_db.raw.player_games p ON g.game_url = p.url
    WHERE (g.white_username IN (SELECT player FROM players) OR g.black_username IN (SELECT player FROM players))
      AND p.eco IS NOT NULL
),
player_openings AS (
    SELECT 
        white_username as player,
        'white' as color,
        opening_short,
        CASE WHEN winner = white_username THEN 1 ELSE 0 END as won,
        CASE WHEN winner IS NOT NULL THEN 1 ELSE 0 END as decisive
    FROM games_with_opening
    WHERE white_username IN (SELECT player FROM players)
    
    UNION ALL
    
    SELECT 
        black_username as player,
        'black' as color,
        opening_short,
        CASE WHEN winner = black_username THEN 1 ELSE 0 END as won,
        CASE WHEN winner IS NOT NULL THEN 1 ELSE 0 END as decisive
    FROM games_with_opening
    WHERE black_username IN (SELECT player FROM players)
)
SELECT 
    player,
    opening_short,
    COUNT(*) as games,
    ROUND(100.0 * SUM(won) / NULLIF(SUM(decisive), 0), 2) as win_rate,
    color
FROM player_openings
WHERE opening_short IS NOT NULL AND opening_short != ' '
GROUP BY player, opening_short, color
HAVING COUNT(*) >= 100
ORDER BY player, games DESC
