-- Game Insights: enriched analytics model
-- Reveals patterns in game outcomes, upsets, and player behavior

SELECT
  *,
  
  -- Time dimensions for analysis
  EXTRACT(HOUR FROM end_time) AS hour_utc,
  EXTRACT(DOW FROM end_time) AS day_of_week,
  STRFTIME(end_time, '%A') AS day_name,
  
  -- Upset detection: lower-rated player wins
  CASE 
    WHEN white_result = 'win' AND rating_diff < -100 THEN 'White Upset (underdog wins)'
    WHEN black_result = 'win' AND rating_diff > 100 THEN 'Black Upset (underdog wins)'
    WHEN white_result = 'win' AND rating_diff > 100 THEN 'Favorite wins (white)'
    WHEN black_result = 'win' AND rating_diff < -100 THEN 'Favorite wins (black)'
    WHEN winner IS NULL THEN 'Draw'
    ELSE 'Close match'
  END AS upset_category,
  
  -- Was this an upset? (underdog wins by 100+ rating points)
  CASE 
    WHEN white_result = 'win' AND rating_diff < -100 THEN TRUE
    WHEN black_result = 'win' AND rating_diff > 100 THEN TRUE
    ELSE FALSE
  END AS is_upset,
  
  -- Rating gap buckets
  CASE
    WHEN ABS(rating_diff) < 50 THEN 'Even match (±50)'
    WHEN ABS(rating_diff) < 100 THEN 'Slight edge (50-100)'
    WHEN ABS(rating_diff) < 200 THEN 'Clear favorite (100-200)'
    WHEN ABS(rating_diff) < 400 THEN 'Big mismatch (200-400)'
    ELSE 'Massive gap (400+)'
  END AS rating_gap_bucket,
  
  -- Game termination type
  CASE
    WHEN white_result = 'checkmated' OR black_result = 'checkmated' THEN 'Checkmate'
    WHEN white_result = 'resigned' OR black_result = 'resigned' THEN 'Resignation'
    WHEN white_result = 'timeout' OR black_result = 'timeout' THEN 'Timeout'
    WHEN white_result = 'abandoned' OR black_result = 'abandoned' THEN 'Abandoned'
    WHEN white_result IN ('stalemate', 'agreed', 'repetition', 'insufficient', '50move') 
      OR black_result IN ('stalemate', 'agreed', 'repetition', 'insufficient', '50move') THEN 'Draw'
    ELSE 'Other'
  END AS termination_type,
  
  -- Who was favored?
  CASE
    WHEN rating_diff > 50 THEN 'White favored'
    WHEN rating_diff < -50 THEN 'Black favored'
    ELSE 'Even'
  END AS pre_game_favorite,
  
  -- Did the favorite win?
  CASE
    WHEN rating_diff > 50 AND white_result = 'win' THEN 'Favorite won'
    WHEN rating_diff < -50 AND black_result = 'win' THEN 'Favorite won'
    WHEN rating_diff > 50 AND black_result = 'win' THEN 'Underdog won'
    WHEN rating_diff < -50 AND white_result = 'win' THEN 'Underdog won'
    WHEN winner IS NULL THEN 'Draw'
    ELSE 'Even match'
  END AS outcome_vs_expectation,
  
  -- Average rating of the game
  (white_rating + black_rating) / 2 AS avg_game_rating,
  
  -- Rating tier of the game
  CASE
    WHEN (white_rating + black_rating) / 2 >= 2500 THEN 'Super GM (2500+)'
    WHEN (white_rating + black_rating) / 2 >= 2200 THEN 'Master (2200-2500)'
    WHEN (white_rating + black_rating) / 2 >= 1800 THEN 'Expert (1800-2200)'
    WHEN (white_rating + black_rating) / 2 >= 1400 THEN 'Intermediate (1400-1800)'
    ELSE 'Beginner (<1400)'
  END AS rating_tier

FROM read_parquet('data/games_enriched.parquet')
WHERE end_time IS NOT NULL
