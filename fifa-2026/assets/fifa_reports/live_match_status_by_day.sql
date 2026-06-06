/* @bruin
name: fifa_reports.live_match_status_by_day
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Daily fixture counts by latest normalized match status. Used for the
  dashboard's stacked tournament-progress chart.

depends:
  - fifa_staging.live_matches

materialization:
  type: table
  strategy: create+replace

columns:
  - name: match_date
    type: DATE
    description: Scheduled match date in source local venue time.
    primary_key: true
  - name: scheduled_matches
    type: INTEGER
    description: Count of fixtures not yet started on the date.
  - name: live_matches
    type: INTEGER
    description: Count of fixtures currently live on the date.
  - name: completed_matches
    type: INTEGER
    description: Count of fixtures marked finished on the date.
  - name: total_matches
    type: INTEGER
    description: Count of fixtures on the date.

@bruin */

SELECT
  match_date,
  COUNTIF(match_status = 'scheduled') AS scheduled_matches,
  COUNTIF(match_status = 'live') AS live_matches,
  COUNTIF(match_status = 'completed') AS completed_matches,
  COUNT(*) AS total_matches
FROM `bruin-playground-arsalan.fifa_staging.live_matches`
GROUP BY match_date
ORDER BY match_date
