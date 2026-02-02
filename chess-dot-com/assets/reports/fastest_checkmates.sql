-- Fastest checkmates: Games that ended in checkmate with fewest moves
WITH players AS (
    SELECT unnest(ARRAY[
        'francisbegbie', 'Castlecard', 'JolinTsai', 'SeanWinshand', 'GutovAndrey',
        'ManuDavid2910', 'Hikaru', 'ArkadiiKhromaev', 'Philippians46', 'nihalsarin',
        'MagnusCarlsen', 'FabianoCaruana', 'IanNepomniachtchi', 'AnishGiri', 'alireza2003',
        'LevonAronian', 'GothamChess', 'AlexandraBotez', 'EricRosen', 'Firouzja2003'
    ]) as player
),
games_with_checkmate AS (
    SELECT 
        g.white_username,
        g.black_username,
        g.winner,
        g.time_class,
        g.game_url,
        g.white_result,
        g.black_result,
        (LENGTH(p.pgn) - LENGTH(REPLACE(p.pgn, '. ', ''))) / 2 as approx_moves
    FROM my_db.staging.games_enriched g
    JOIN my_db.raw.player_games p ON g.game_url = p.url
    WHERE (g.white_username IN (SELECT player FROM players) OR g.black_username IN (SELECT player FROM players))
      AND (g.white_result = 'checkmated' OR g.black_result = 'checkmated')
      AND p.pgn IS NOT NULL
)
SELECT 
    winner,
    CASE WHEN white_result = 'checkmated' THEN white_username ELSE black_username END as checkmated_player,
    approx_moves as move_count,
    time_class,
    game_url
FROM games_with_checkmate
WHERE approx_moves > 0 AND approx_moves <= 25
ORDER BY approx_moves ASC
LIMIT 15
