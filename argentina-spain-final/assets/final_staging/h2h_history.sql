/* @bruin
name: final_staging.h2h_history
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Latest deduplicated senior men's Argentina–Spain historical head-to-head
  series from 11v11, plus the 2026 final from FIFA's primary match report,
  normalized into Argentina-perspective results.

depends:
  - final_raw.h2h_history
  - final_raw.fifa_match_facts

materialization:
  type: table
  strategy: create+replace

columns:
  - name: match_date
    type: DATE
    description: Match date.
    primary_key: true
  - name: home_team
    type: VARCHAR
    description: Home team.
  - name: away_team
    type: VARCHAR
    description: Away team.
  - name: competition
    type: VARCHAR
    description: Competition from 11v11 or FIFA's final report.
  - name: venue
    type: VARCHAR
    description: Venue and city from the cited match source.
  - name: scoreline
    type: VARCHAR
    description: Home-away scoreline.
  - name: argentina_outcome
    type: VARCHAR
    description: Argentina-perspective W, D, or L result.
  - name: source_url
    type: VARCHAR
    description: Cited 11v11 or FIFA match-report source.

@bruin */

WITH historical AS (
  SELECT *
  FROM `bruin-playground-arsalan.final_raw.h2h_history`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY match_date, home_team, away_team ORDER BY extracted_at DESC, source_hash DESC) = 1
),
final_facts AS (
  SELECT *
  FROM `bruin-playground-arsalan.final_raw.fifa_match_facts`
  WHERE stage = 'Final'
    AND match_id = '104'
    AND fact_type = 'team_metric'
    AND team_name IN ('Argentina', 'Spain')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY match_id, team_name, metric_name
    ORDER BY extracted_at DESC, source_hash DESC
  ) = 1
),
fifa_final AS (
  SELECT
    ANY_VALUE(match_date) AS match_date,
    'Spain' AS home_team,
    'Argentina' AS away_team,
    'FIFA World Cup' AS competition,
    ANY_VALUE(venue) AS venue,
    CAST(MAX(IF(team_name = 'Spain' AND metric_name = 'goals', numeric_value, NULL)) AS INTEGER) AS home_goals,
    CAST(MAX(IF(team_name = 'Argentina' AND metric_name = 'goals', numeric_value, NULL)) AS INTEGER) AS away_goals,
    'L' AS argentina_outcome,
    ANY_VALUE(source_url) AS source_url
  FROM final_facts
  HAVING COUNT(DISTINCT team_name) = 2
),
all_matches AS (
  SELECT
    match_date,
    home_team,
    away_team,
    competition,
    venue,
    home_goals,
    away_goals,
    argentina_outcome,
    source_url,
    2 AS source_priority
  FROM historical
  UNION ALL
  SELECT
    match_date,
    home_team,
    away_team,
    competition,
    venue,
    home_goals,
    away_goals,
    argentina_outcome,
    source_url,
    1 AS source_priority
  FROM fifa_final
)

SELECT
  match_date,
  home_team,
  away_team,
  competition,
  venue,
  CONCAT(CAST(home_goals AS STRING), '-', CAST(away_goals AS STRING)) AS scoreline,
  argentina_outcome,
  source_url
FROM all_matches
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY match_date, home_team, away_team
  ORDER BY source_priority
) = 1
ORDER BY match_date
