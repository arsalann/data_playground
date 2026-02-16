-- Head-to-Head: pairwise records between tracked players
WITH tracked_players AS (
    SELECT username FROM my_db.raw.player_profiles
)
SELECT
    LEAST(white_username, black_username) AS player1,
    GREATEST(white_username, black_username) AS player2,
    SUM(CASE WHEN winner = LEAST(white_username, black_username) THEN 1 ELSE 0 END) AS p1_wins,
    SUM(CASE WHEN winner = GREATEST(white_username, black_username) THEN 1 ELSE 0 END) AS p2_wins,
    SUM(CASE WHEN winner IS NULL THEN 1 ELSE 0 END) AS draws,
    COUNT(*) AS total_games,
    ROUND(100.0 * SUM(CASE WHEN winner = LEAST(white_username, black_username) THEN 1 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN winner IS NOT NULL THEN 1 ELSE 0 END), 0), 1) AS p1_win_pct
FROM my_db.staging.games_enriched
WHERE (
    LOWER(white_username) IN (SELECT LOWER(username) FROM tracked_players)
    OR LOWER(black_username) IN (SELECT LOWER(username) FROM tracked_players)
)
GROUP BY 1, 2
HAVING COUNT(*) >= 3
ORDER BY total_games DESC
