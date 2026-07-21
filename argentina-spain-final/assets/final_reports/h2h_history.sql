/* @bruin
name: final_reports.h2h_history
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Presentation-ready complete 11v11 senior-men’s historical Argentina–Spain
  series, retaining the secondary-source provenance.

depends:
  - final_staging.h2h_history

materialization:
  type: table
  strategy: create+replace

columns:
  - name: match_date
    type: DATE
    description: Match date.
    primary_key: true
  - name: fixture
    type: VARCHAR
    description: Home versus away fixture label.
  - name: competition
    type: VARCHAR
    description: 11v11 competition label.
  - name: venue
    type: VARCHAR
    description: Venue and city.
  - name: scoreline
    type: VARCHAR
    description: Home-away scoreline.
  - name: argentina_outcome
    type: VARCHAR
    description: Argentina-perspective W, D, or L.

@bruin */

SELECT
  match_date,
  CONCAT(home_team, ' v ', away_team) AS fixture,
  competition,
  venue,
  scoreline,
  argentina_outcome
FROM `bruin-playground-arsalan.final_staging.h2h_history`
ORDER BY match_date
