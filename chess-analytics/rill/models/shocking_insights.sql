-- Shocking Insights: The data that makes you question everything about chess
-- Built from real patterns found in 136K+ games

WITH gm_games AS (
  SELECT 
    *,
    CASE 
      WHEN white_username IN ('MagnusCarlsen', 'Hikaru', 'GothamChess', 'DanielNaroditsky') THEN white_username
      ELSE black_username
    END as gm_player,
    CASE 
      WHEN white_username IN ('MagnusCarlsen', 'Hikaru', 'GothamChess', 'DanielNaroditsky') THEN white_rating
      ELSE black_rating
    END as gm_rating,
    CASE 
      WHEN white_username NOT IN ('MagnusCarlsen', 'Hikaru', 'GothamChess', 'DanielNaroditsky') THEN white_username
      ELSE black_username
    END as opponent,
    CASE 
      WHEN white_username NOT IN ('MagnusCarlsen', 'Hikaru', 'GothamChess', 'DanielNaroditsky') THEN white_rating
      ELSE black_rating
    END as opponent_rating,
    CASE 
      WHEN winner IN ('MagnusCarlsen', 'Hikaru', 'GothamChess', 'DanielNaroditsky') THEN 'GM Won'
      WHEN winner IS NULL THEN 'Draw'
      ELSE 'GM Lost'
    END as gm_outcome,
    -- Time dimensions
    EXTRACT(HOUR FROM end_time) as hour_utc,
    CASE EXTRACT(DOW FROM end_time)
      WHEN 0 THEN 'Sunday'
      WHEN 1 THEN 'Monday'  
      WHEN 2 THEN 'Tuesday'
      WHEN 3 THEN 'Wednesday'
      WHEN 4 THEN 'Thursday'
      WHEN 5 THEN 'Friday'
      WHEN 6 THEN 'Saturday'
    END as day_of_week,
    -- Loss type for GMs
    CASE 
      WHEN white_username IN ('MagnusCarlsen', 'Hikaru', 'GothamChess', 'DanielNaroditsky') 
        AND white_result IN ('checkmated', 'resigned', 'timeout') THEN white_result
      WHEN black_username IN ('MagnusCarlsen', 'Hikaru', 'GothamChess', 'DanielNaroditsky')
        AND black_result IN ('checkmated', 'resigned', 'timeout') THEN black_result
      ELSE NULL
    END as gm_loss_type
  FROM games_enriched
  WHERE white_username IN ('MagnusCarlsen', 'Hikaru', 'GothamChess', 'DanielNaroditsky')
     OR black_username IN ('MagnusCarlsen', 'Hikaru', 'GothamChess', 'DanielNaroditsky')
)

SELECT 
  *,
  -- Is this an upset? Opponent 300+ rating below GM wins
  CASE WHEN gm_outcome = 'GM Lost' AND (gm_rating - opponent_rating) > 300 THEN TRUE ELSE FALSE END as is_giant_kill,
  gm_rating - opponent_rating as rating_advantage,
  -- Rating gap buckets
  CASE 
    WHEN gm_rating - opponent_rating > 1500 THEN '1500+ (Streamer Bait)'
    WHEN gm_rating - opponent_rating > 1000 THEN '1000-1500 (Massive)'
    WHEN gm_rating - opponent_rating > 500 THEN '500-1000 (Large)'
    WHEN gm_rating - opponent_rating > 200 THEN '200-500 (Moderate)'
    ELSE '<200 (Competitive)'
  END as skill_gap_bucket,
  -- Time period
  CASE 
    WHEN hour_utc BETWEEN 6 AND 11 THEN 'Morning (6-11 UTC)'
    WHEN hour_utc BETWEEN 12 AND 17 THEN 'Afternoon (12-17 UTC)'
    WHEN hour_utc BETWEEN 18 AND 23 THEN 'Evening (18-23 UTC)'
    ELSE 'Late Night (0-5 UTC)'
  END as time_of_day
FROM gm_games
