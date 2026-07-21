/* @bruin
name: final_reports.phase_mix
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Mean FIFA phase shares across completed pre-final reports. FIFA phase shares
  may overlap, so this table supports grouped bars rather than invalid stacks.

depends:
  - final_staging.team_match_phases

materialization:
  type: table
  strategy: create+replace

columns:
  - name: team_name
    type: VARCHAR
    description: Argentina or Spain.
    primary_key: true
  - name: phase_group
    type: VARCHAR
    description: In-possession or out-of-possession phase family.
    primary_key: true
  - name: phase_name
    type: VARCHAR
    description: FIFA phase label.
    primary_key: true
  - name: mean_phase_share_pct
    type: DOUBLE
    description: Mean separate phase share percentage across seven reports.
  - name: completed_matches
    type: INTEGER
    description: Number of reports contributing to the mean.

@bruin */

SELECT
  team_name,
  phase_group,
  phase_name,
  AVG(phase_share_pct) AS mean_phase_share_pct,
  COUNT(*) AS completed_matches
FROM `bruin-playground-arsalan.final_staging.team_match_phases`
GROUP BY team_name, phase_group, phase_name
HAVING completed_matches = 7
ORDER BY phase_group, phase_name, team_name
