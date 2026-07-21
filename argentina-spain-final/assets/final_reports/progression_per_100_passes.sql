/* @bruin
name: final_reports.progression_per_100_passes
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Completed line breaks and ball progressions per 100 attempted passes across
  the seven completed pre-final reports for each finalist.

depends:
  - final_staging.team_match_metrics

materialization:
  type: table
  strategy: create+replace

columns:
  - name: team_name
    type: VARCHAR
    description: Argentina or Spain.
    primary_key: true
  - name: completed_line_breaks_per_100_passes
    type: DOUBLE
    description: Completed line breaks per 100 total passes.
  - name: ball_progressions_per_100_passes
    type: DOUBLE
    description: Ball progressions per 100 total passes.
  - name: completed_matches
    type: INTEGER
    description: Completed pre-final reports included.

@bruin */

SELECT
  team_name,
  SAFE_DIVIDE(SUM(completed_line_breaks) * 100, SUM(total_passes)) AS completed_line_breaks_per_100_passes,
  SAFE_DIVIDE(SUM(ball_progressions) * 100, SUM(total_passes)) AS ball_progressions_per_100_passes,
  COUNT(*) AS completed_matches
FROM `bruin-playground-arsalan.final_staging.team_match_metrics`
WHERE team_name IN ('Argentina', 'Spain')
GROUP BY team_name
HAVING completed_matches = 7
ORDER BY team_name
