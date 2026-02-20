WITH gloomy_flags AS (
    SELECT
        date,
        sunshine_hours < 1 AS is_gloomy
    FROM `bruin-playground-arsalan.staging.weather_daily`
),

streaks AS (
    SELECT
        date,
        is_gloomy,
        SUM(CASE WHEN NOT is_gloomy THEN 1 ELSE 0 END)
            OVER (ORDER BY date ROWS UNBOUNDED PRECEDING) AS streak_group
    FROM gloomy_flags
),

streak_lengths AS (
    SELECT
        streak_group,
        COUNT(*) AS streak_length,
        MIN(date) AS streak_start,
        MAX(date) AS streak_end
    FROM streaks
    WHERE is_gloomy
    GROUP BY streak_group
)

SELECT streak_length, streak_start, streak_end
FROM streak_lengths
ORDER BY streak_length DESC
LIMIT 15;
