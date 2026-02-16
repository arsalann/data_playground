-- Streamer vs GM: compare streamer performance against super GM performance
WITH tracked_players AS (
    SELECT username FROM my_db.raw.player_profiles
),
player_categories AS (
    SELECT username,
        CASE
            WHEN LOWER(username) IN ('gothamchess', 'alexandrabotez', 'imrosen', 'chessbrah', 'annacramling')
            THEN 'Streamer'
            ELSE 'Super GM'
        END AS category
    FROM tracked_players
),
player_games AS (
    SELECT
        white_username AS player,
        CASE WHEN winner = white_username THEN 1 ELSE 0 END AS won,
        CASE WHEN winner IS NOT NULL AND winner != white_username THEN 1 ELSE 0 END AS lost,
        CASE WHEN winner IS NULL THEN 1 ELSE 0 END AS drew,
        white_rating AS rating,
        time_class,
        white_result AS result
    FROM my_db.staging.games_enriched
    WHERE LOWER(white_username) IN (SELECT LOWER(username) FROM tracked_players)

    UNION ALL

    SELECT
        black_username AS player,
        CASE WHEN winner = black_username THEN 1 ELSE 0 END AS won,
        CASE WHEN winner IS NOT NULL AND winner != black_username THEN 1 ELSE 0 END AS lost,
        CASE WHEN winner IS NULL THEN 1 ELSE 0 END AS drew,
        black_rating AS rating,
        time_class,
        black_result AS result
    FROM my_db.staging.games_enriched
    WHERE LOWER(black_username) IN (SELECT LOWER(username) FROM tracked_players)
)
SELECT
    pg.player,
    pc.category,
    COUNT(*) AS total_games,
    ROUND(100.0 * SUM(pg.won) / COUNT(*), 2) AS win_rate,
    ROUND(AVG(pg.rating), 0) AS avg_rating,
    MAX(pg.rating) AS peak_rating,
    ROUND(100.0 * SUM(CASE WHEN pg.lost = 1 AND pg.result = 'checkmated' THEN 1 ELSE 0 END) /
        NULLIF(COUNT(*), 0), 2) AS checkmate_loss_rate,
    ROUND(100.0 * SUM(CASE WHEN pg.lost = 1 AND pg.result = 'resigned' THEN 1 ELSE 0 END) /
        NULLIF(COUNT(*), 0), 2) AS resign_loss_rate,
    SUM(CASE WHEN time_class = 'bullet' THEN 1 ELSE 0 END) AS bullet_games,
    SUM(CASE WHEN time_class = 'blitz' THEN 1 ELSE 0 END) AS blitz_games,
    SUM(CASE WHEN time_class = 'rapid' THEN 1 ELSE 0 END) AS rapid_games
FROM player_games pg
JOIN player_categories pc ON LOWER(pg.player) = LOWER(pc.username)
GROUP BY pg.player, pc.category
HAVING COUNT(*) >= 3
ORDER BY pc.category, win_rate DESC
