/* @bruin
name: fifa_reports.live_group_summary
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Group-level progress and goal summary from the latest match states. Used by
  the overall tournament summary tab.

depends:
  - fifa_staging.live_matches

materialization:
  type: table
  strategy: create+replace

columns:
  - name: group_id
    type: VARCHAR
    description: Group letter A-L.
    primary_key: true
  - name: scheduled
    type: INTEGER
    description: Fixtures not yet started in the group.
  - name: live
    type: INTEGER
    description: Fixtures currently live in the group.
  - name: completed
    type: INTEGER
    description: Fixtures marked finished in the group.
  - name: total_matches
    type: INTEGER
    description: Total group-stage fixtures in the group.
  - name: total_goals
    type: INTEGER
    description: Goals scored in latest match states for the group.
  - name: goals_per_completed_match
    type: DOUBLE
    description: Average goals per completed fixture in the group.

@bruin */

SELECT
  group_id,
  COUNTIF(match_status = 'scheduled') AS scheduled,
  COUNTIF(match_status = 'live') AS live,
  COUNTIF(match_status = 'completed') AS completed,
  COUNT(*) AS total_matches,
  SUM(total_goals) AS total_goals,
  ROUND(SAFE_DIVIDE(SUM(total_goals), COUNTIF(match_status = 'completed')), 2) AS goals_per_completed_match
FROM `bruin-playground-arsalan.fifa_staging.live_matches`
WHERE match_type = 'group'
  AND group_id IS NOT NULL
GROUP BY group_id
ORDER BY group_id
