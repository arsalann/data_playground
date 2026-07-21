/* @bruin
name: final_staging.h2h_history
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Latest deduplicated senior men's Argentina–Spain historical head-to-head
  series from 11v11, normalized into Argentina-perspective results.

depends:
  - final_raw.h2h_history

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
    description: Competition from 11v11.
  - name: venue
    type: VARCHAR
    description: Venue and city from linked 11v11 match page.
  - name: scoreline
    type: VARCHAR
    description: Home-away scoreline.
  - name: argentina_outcome
    type: VARCHAR
    description: Argentina-perspective W, D, or L result.
  - name: source_url
    type: VARCHAR
    description: 11v11 record source.

@bruin */

WITH deduped AS (
  SELECT *
  FROM `bruin-playground-arsalan.final_raw.h2h_history`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY match_date, home_team, away_team ORDER BY extracted_at DESC, source_hash DESC) = 1
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
FROM deduped
ORDER BY match_date
