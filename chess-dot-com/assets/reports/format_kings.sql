-- Format Kings: performance breakdown by time control (bullet/blitz/rapid)
WITH tracked_players AS (
    SELECT username FROM my_db.raw.player_profiles
),
player_games AS (
    SELECT
        white_username AS player,
        time_class,
        white_rating AS rating,
        CASE WHEN winner = white_username THEN 1 ELSE 0 END AS won,
        CASE WHEN winner IS NOT NULL AND winner != white_username THEN 1 ELSE 0 END AS lost,
        CASE WHEN winner IS NULL THEN 1 ELSE 0 END AS drew
    FROM my_db.staging.games_enriched
    WHERE LOWER(white_username) IN (SELECT LOWER(username) FROM tracked_players)

    UNION ALL

    SELECT
        black_username AS player,
        time_class,
        black_rating AS rating,
        CASE WHEN winner = black_username THEN 1 ELSE 0 END AS won,
        CASE WHEN winner IS NOT NULL AND winner != black_username THEN 1 ELSE 0 END AS lost,
        CASE WHEN winner IS NULL THEN 1 ELSE 0 END AS drew
    FROM my_db.staging.games_enriched
    WHERE LOWER(black_username) IN (SELECT LOWER(username) FROM tracked_players)
)
SELECT
    player,
    time_class,
    COUNT(*) AS games,
    SUM(won) AS wins,
    SUM(lost) AS losses,
    SUM(drew) AS draws,
    ROUND(100.0 * SUM(won) / COUNT(*), 2) AS win_rate,
    ROUND(AVG(rating), 0) AS avg_rating,
    MAX(rating) AS peak_rating
FROM player_games
GROUP BY player, time_class
HAVING COUNT(*) >= 3
ORDER BY player, games DESC
