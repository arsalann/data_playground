-- Head-to-Head Rivalries
-- Track matchups between specific players

WITH matchups AS (
  SELECT
    CASE WHEN white_username < black_username 
      THEN white_username ELSE black_username END AS player_1,
    CASE WHEN white_username < black_username 
      THEN black_username ELSE white_username END AS player_2,
    time_class,
    CASE 
      WHEN white_username < black_username AND white_result = 'win' THEN 'player_1_wins'
      WHEN white_username > black_username AND black_result = 'win' THEN 'player_1_wins'
      WHEN white_username < black_username AND black_result = 'win' THEN 'player_2_wins'
      WHEN white_username > black_username AND white_result = 'win' THEN 'player_2_wins'
      ELSE 'draw'
    END AS result,
    end_time
  FROM read_parquet('data/games_enriched.parquet')
  WHERE white_username IS NOT NULL AND black_username IS NOT NULL
)

SELECT
  player_1,
  player_2,
  player_1 || ' vs ' || player_2 AS matchup_name,
  time_class,
  COUNT(*) AS total_games,
  SUM(CASE WHEN result = 'player_1_wins' THEN 1 ELSE 0 END) AS player_1_wins,
  SUM(CASE WHEN result = 'player_2_wins' THEN 1 ELSE 0 END) AS player_2_wins,
  SUM(CASE WHEN result = 'draw' THEN 1 ELSE 0 END) AS draws,
  ROUND(100.0 * SUM(CASE WHEN result = 'player_1_wins' THEN 1 ELSE 0 END) / COUNT(*), 1) AS player_1_win_pct,
  MIN(end_time) AS first_game,
  MAX(end_time) AS last_game
FROM matchups
GROUP BY player_1, player_2, time_class
HAVING COUNT(*) >= 2
ORDER BY total_games DESC
