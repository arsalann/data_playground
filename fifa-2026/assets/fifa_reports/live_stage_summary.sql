/* @bruin
name: fifa_reports.live_stage_summary
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Tournament progress broken down by stage. Used by the overall tournament
  summary tab to show scheduled, live, and completed matches by phase.

depends:
  - fifa_staging.live_matches

materialization:
  type: table
  strategy: create+replace

columns:
  - name: stage_label
    type: VARCHAR
    description: Human-readable tournament stage label.
    primary_key: true
  - name: stage_order
    type: INTEGER
    description: Display order for tournament stages.
  - name: scheduled
    type: INTEGER
    description: Fixtures not yet started in the stage.
  - name: live
    type: INTEGER
    description: Fixtures currently live in the stage.
  - name: completed
    type: INTEGER
    description: Fixtures marked finished in the stage.
  - name: total_matches
    type: INTEGER
    description: Total fixtures in the stage.

@bruin */

SELECT
  stage_label,
  CASE stage_label
    WHEN 'Group stage' THEN 1
    WHEN 'Round of 32' THEN 2
    WHEN 'Round of 16' THEN 3
    WHEN 'Quarter-final' THEN 4
    WHEN 'Semi-final' THEN 5
    WHEN 'Third-place match' THEN 6
    WHEN 'Final' THEN 7
    ELSE 99
  END AS stage_order,
  COUNTIF(match_status = 'scheduled') AS scheduled,
  COUNTIF(match_status = 'live') AS live,
  COUNTIF(match_status = 'completed') AS completed,
  COUNT(*) AS total_matches
FROM `bruin-playground-arsalan.fifa_staging.live_matches`
GROUP BY stage_label, stage_order
ORDER BY stage_order
