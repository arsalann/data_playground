-- Rating Evolution: daily rating progression per player
WITH tracked_players AS (
    SELECT username FROM my_db.raw.player_profiles
),
player_ratings AS (
    SELECT
        white_username AS player,
        white_rating AS rating,
        time_class,
        CAST(end_time AS DATE) AS game_date,
        end_time
    FROM my_db.staging.games_enriched
    WHERE LOWER(white_username) IN (SELECT LOWER(username) FROM tracked_players)

    UNION ALL

    SELECT
        black_username AS player,
        black_rating AS rating,
        time_class,
        CAST(end_time AS DATE) AS game_date,
        end_time
    FROM my_db.staging.games_enriched
    WHERE LOWER(black_username) IN (SELECT LOWER(username) FROM tracked_players)
),
daily_ratings AS (
    SELECT
        player,
        time_class,
        game_date,
        -- Use the last rating of each day as the "closing" rating
        LAST_VALUE(rating) OVER (
            PARTITION BY player, time_class, game_date
            ORDER BY end_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS closing_rating,
        MAX(rating) OVER (PARTITION BY player, time_class, game_date) AS daily_high,
        MIN(rating) OVER (PARTITION BY player, time_class, game_date) AS daily_low,
        COUNT(*) OVER (PARTITION BY player, time_class, game_date) AS games_played,
        ROW_NUMBER() OVER (PARTITION BY player, time_class, game_date ORDER BY end_time DESC) AS rn
    FROM player_ratings
)
SELECT
    player,
    time_class,
    game_date,
    closing_rating,
    daily_high,
    daily_low,
    games_played
FROM daily_ratings
WHERE rn = 1
ORDER BY player, time_class, game_date
