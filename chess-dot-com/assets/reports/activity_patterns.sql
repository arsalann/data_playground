-- Activity Patterns: when do players play? Hour of day and day of week
WITH tracked_players AS (
    SELECT username FROM my_db.raw.player_profiles
),
player_activity AS (
    SELECT
        white_username AS player,
        end_time,
        EXTRACT(hour FROM end_time) AS hour_utc,
        EXTRACT(dow FROM end_time) AS day_of_week,
        CASE WHEN winner = white_username THEN 1 ELSE 0 END AS won,
        CASE WHEN winner IS NOT NULL THEN 1 ELSE 0 END AS decisive
    FROM my_db.staging.games_enriched
    WHERE LOWER(white_username) IN (SELECT LOWER(username) FROM tracked_players)

    UNION ALL

    SELECT
        black_username AS player,
        end_time,
        EXTRACT(hour FROM end_time) AS hour_utc,
        EXTRACT(dow FROM end_time) AS day_of_week,
        CASE WHEN winner = black_username THEN 1 ELSE 0 END AS won,
        CASE WHEN winner IS NOT NULL THEN 1 ELSE 0 END AS decisive
    FROM my_db.staging.games_enriched
    WHERE LOWER(black_username) IN (SELECT LOWER(username) FROM tracked_players)
)
SELECT
    player,
    hour_utc,
    day_of_week,
    COUNT(*) AS games,
    SUM(won) AS wins,
    ROUND(100.0 * SUM(won) / NULLIF(SUM(decisive), 0), 2) AS win_rate
FROM player_activity
GROUP BY player, hour_utc, day_of_week
ORDER BY player, hour_utc, day_of_week
