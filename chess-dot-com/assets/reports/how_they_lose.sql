-- How They Lose: breakdown of loss types per player (resign/timeout/checkmate)
WITH tracked_players AS (
    SELECT username FROM my_db.raw.player_profiles
),
player_results AS (
    SELECT
        white_username AS player,
        white_result AS result,
        winner IS NOT NULL AND winner != white_username AS lost
    FROM my_db.staging.games_enriched
    WHERE LOWER(white_username) IN (SELECT LOWER(username) FROM tracked_players)

    UNION ALL

    SELECT
        black_username AS player,
        black_result AS result,
        winner IS NOT NULL AND winner != black_username AS lost
    FROM my_db.staging.games_enriched
    WHERE LOWER(black_username) IN (SELECT LOWER(username) FROM tracked_players)
)
SELECT
    player,
    COUNT(*) AS total_games,
    SUM(CASE WHEN lost THEN 1 ELSE 0 END) AS total_losses,
    SUM(CASE WHEN lost AND result = 'resigned' THEN 1 ELSE 0 END) AS resigned,
    SUM(CASE WHEN lost AND result = 'timeout' THEN 1 ELSE 0 END) AS lost_on_time,
    SUM(CASE WHEN lost AND result = 'checkmated' THEN 1 ELSE 0 END) AS got_checkmated,
    SUM(CASE WHEN lost AND result = 'abandoned' THEN 1 ELSE 0 END) AS abandoned,
    -- Percentages of total losses
    ROUND(100.0 * SUM(CASE WHEN lost AND result = 'resigned' THEN 1 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN lost THEN 1 ELSE 0 END), 0), 2) AS resign_pct,
    ROUND(100.0 * SUM(CASE WHEN lost AND result = 'timeout' THEN 1 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN lost THEN 1 ELSE 0 END), 0), 2) AS timeout_pct,
    ROUND(100.0 * SUM(CASE WHEN lost AND result = 'checkmated' THEN 1 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN lost THEN 1 ELSE 0 END), 0), 2) AS checkmate_pct
FROM player_results
GROUP BY player
HAVING SUM(CASE WHEN lost THEN 1 ELSE 0 END) >= 2
ORDER BY total_losses DESC
