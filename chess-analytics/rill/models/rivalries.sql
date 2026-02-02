-- Epic Rivalries: The battles that define online chess

WITH all_matchups AS (
  SELECT
    CASE WHEN white_username < black_username THEN white_username ELSE black_username END as player_1,
    CASE WHEN white_username < black_username THEN black_username ELSE white_username END as player_2,
    winner,
    time_class,
    end_time,
    white_rating,
    black_rating
  FROM read_parquet('data/games_enriched.parquet')
  WHERE winner IS NOT NULL
)

SELECT
  player_1 || ' vs ' || player_2 as rivalry,
  player_1,
  player_2,
  time_class,
  COUNT(*) as total_battles,
  SUM(CASE WHEN winner = player_1 THEN 1 ELSE 0 END) as p1_wins,
  SUM(CASE WHEN winner = player_2 THEN 1 ELSE 0 END) as p2_wins,
  SUM(CASE WHEN winner = player_1 THEN 1 ELSE 0 END) - SUM(CASE WHEN winner = player_2 THEN 1 ELSE 0 END) as win_differential,
  ROUND(CAST(SUM(CASE WHEN winner = player_1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*), 3) as p1_win_rate,
  -- Who dominates?
  CASE 
    WHEN SUM(CASE WHEN winner = player_1 THEN 1 ELSE 0 END) > SUM(CASE WHEN winner = player_2 THEN 1 ELSE 0 END) * 1.5 
      THEN player_1 || ' DOMINATES'
    WHEN SUM(CASE WHEN winner = player_2 THEN 1 ELSE 0 END) > SUM(CASE WHEN winner = player_1 THEN 1 ELSE 0 END) * 1.5 
      THEN player_2 || ' DOMINATES'
    ELSE 'CLOSE RIVALRY'
  END as rivalry_status,
  MIN(end_time) as first_battle,
  MAX(end_time) as last_battle
FROM all_matchups
GROUP BY player_1, player_2, time_class
HAVING COUNT(*) >= 50
ORDER BY total_battles DESC
