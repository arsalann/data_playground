-- Player Overview: comprehensive stats for each tracked player
WITH tracked_players AS (
    SELECT username FROM my_db.raw.player_profiles
),
player_games AS (
    SELECT
        white_username AS player,
        CASE WHEN winner = white_username THEN 1 ELSE 0 END AS won,
        CASE WHEN winner IS NOT NULL AND winner != white_username THEN 1 ELSE 0 END AS lost,
        CASE WHEN winner IS NULL THEN 1 ELSE 0 END AS drew,
        time_class,
        white_rating AS rating,
        'white' AS color,
        end_time
    FROM my_db.staging.games_enriched
    WHERE LOWER(white_username) IN (SELECT LOWER(username) FROM tracked_players)

    UNION ALL

    SELECT
        black_username AS player,
        CASE WHEN winner = black_username THEN 1 ELSE 0 END AS won,
        CASE WHEN winner IS NOT NULL AND winner != black_username THEN 1 ELSE 0 END AS lost,
        CASE WHEN winner IS NULL THEN 1 ELSE 0 END AS drew,
        time_class,
        black_rating AS rating,
        'black' AS color,
        end_time
    FROM my_db.staging.games_enriched
    WHERE LOWER(black_username) IN (SELECT LOWER(username) FROM tracked_players)
)
SELECT
    pg.player,
    p.name AS display_name,
    TRY_CAST(p.followers AS INTEGER) AS followers,
    COUNT(*) AS total_games,
    SUM(pg.won) AS wins,
    SUM(pg.lost) AS losses,
    SUM(pg.drew) AS draws,
    ROUND(100.0 * SUM(pg.won) / COUNT(*), 2) AS win_rate,
    ROUND(AVG(pg.rating), 0) AS avg_rating,
    MAX(pg.rating) AS peak_rating,
    MIN(pg.rating) AS lowest_rating,
    SUM(CASE WHEN time_class = 'bullet' THEN 1 ELSE 0 END) AS bullet_games,
    SUM(CASE WHEN time_class = 'blitz' THEN 1 ELSE 0 END) AS blitz_games,
    SUM(CASE WHEN time_class = 'rapid' THEN 1 ELSE 0 END) AS rapid_games,
    ROUND(100.0 * SUM(CASE WHEN color = 'white' AND pg.won = 1 THEN 1 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN color = 'white' THEN 1 ELSE 0 END), 0), 2) AS white_win_rate,
    ROUND(100.0 * SUM(CASE WHEN color = 'black' AND pg.won = 1 THEN 1 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN color = 'black' THEN 1 ELSE 0 END), 0), 2) AS black_win_rate,
    MIN(end_time) AS first_game,
    MAX(end_time) AS last_game
FROM player_games pg
LEFT JOIN my_db.raw.player_profiles p ON LOWER(pg.player) = LOWER(p.username)
GROUP BY pg.player, p.name, p.followers
HAVING COUNT(*) >= 5
ORDER BY total_games DESC
