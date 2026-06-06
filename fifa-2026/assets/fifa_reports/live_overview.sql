/* @bruin
name: fifa_reports.live_overview
type: bq.sql
connection: bruin-playground-arsalan
description: |
  One-row KPI table for the FIFA 2026 live tracker. Summarizes the latest
  match snapshot into tournament progress, live/completed/scheduled counts,
  goal totals, and source freshness.

depends:
  - fifa_staging.live_matches

materialization:
  type: table
  strategy: create+replace

columns:
  - name: total_matches
    type: INTEGER
    description: Total fixtures returned by the live source.
  - name: scheduled_matches
    type: INTEGER
    description: Fixtures not yet started.
  - name: live_matches
    type: INTEGER
    description: Fixtures currently not finished and not scheduled.
  - name: completed_matches
    type: INTEGER
    description: Fixtures marked finished by the source.
  - name: total_goals
    type: INTEGER
    description: Total goals across latest match states.
  - name: goals_per_completed_match
    type: DOUBLE
    description: Average goals per completed match.
  - name: next_kickoff_local
    type: TIMESTAMP
    description: Next scheduled kickoff in source local venue time.
  - name: last_source_update_at
    type: TIMESTAMP
    description: Latest source extraction timestamp represented in staging.
  - name: reported_at
    type: TIMESTAMP
    description: Timestamp when the report table was built.

@bruin */

SELECT
  COUNT(*) AS total_matches,
  COUNTIF(match_status = 'scheduled') AS scheduled_matches,
  COUNTIF(match_status = 'live') AS live_matches,
  COUNTIF(match_status = 'completed') AS completed_matches,
  SUM(total_goals) AS total_goals,
  ROUND(SAFE_DIVIDE(SUM(total_goals), COUNTIF(match_status = 'completed')), 2) AS goals_per_completed_match,
  MIN(IF(match_status = 'scheduled', kickoff_local, NULL)) AS next_kickoff_local,
  MAX(source_extracted_at) AS last_source_update_at,
  CURRENT_TIMESTAMP() AS reported_at
FROM `bruin-playground-arsalan.fifa_staging.live_matches`
