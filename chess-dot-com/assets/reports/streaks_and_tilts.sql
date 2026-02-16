-- Streaks & Tilts: longest winning AND losing streaks per player
WITH tracked_players AS (
    SELECT username FROM my_db.raw.player_profiles
),
all_player_games AS (
    SELECT
        white_username AS player,
        end_time,
        CASE WHEN winner = white_username THEN 'win'
             WHEN winner IS NOT NULL THEN 'loss'
             ELSE 'draw' END AS result
    FROM my_db.staging.games_enriched
    WHERE LOWER(white_username) IN (SELECT LOWER(username) FROM tracked_players)

    UNION ALL

    SELECT
        black_username AS player,
        end_time,
        CASE WHEN winner = black_username THEN 'win'
             WHEN winner IS NOT NULL THEN 'loss'
             ELSE 'draw' END AS result
    FROM my_db.staging.games_enriched
    WHERE LOWER(black_username) IN (SELECT LOWER(username) FROM tracked_players)
),
ordered AS (
    SELECT
        player,
        end_time,
        result,
        ROW_NUMBER() OVER (PARTITION BY player ORDER BY end_time) -
        ROW_NUMBER() OVER (PARTITION BY player, result ORDER BY end_time) AS grp
    FROM all_player_games
),
streaks AS (
    SELECT
        player,
        result,
        COUNT(*) AS streak_len,
        MIN(end_time) AS streak_start,
        MAX(end_time) AS streak_end
    FROM ordered
    GROUP BY player, result, grp
)
SELECT
    player,
    MAX(CASE WHEN result = 'win' THEN streak_len ELSE 0 END) AS longest_win_streak,
    MAX(CASE WHEN result = 'loss' THEN streak_len ELSE 0 END) AS longest_loss_streak,
    -- Also get the total number of streaks for context
    SUM(CASE WHEN result = 'win' AND streak_len >= 3 THEN 1 ELSE 0 END) AS hot_streaks_3plus,
    SUM(CASE WHEN result = 'loss' AND streak_len >= 3 THEN 1 ELSE 0 END) AS tilt_streaks_3plus
FROM streaks
GROUP BY player
HAVING MAX(CASE WHEN result = 'win' THEN streak_len ELSE 0 END) > 0
ORDER BY longest_win_streak DESC
