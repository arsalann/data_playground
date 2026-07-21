/* @bruin

name: final_reports.evidence_findings
type: bq.sql
description: |
  Data-driven findings emitted only after both teams have seven complete FIFA
  reports with core metrics. No tactical claim is prewritten into this asset.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - final_reports.team_kpis
  - final_reports.progression_per_100_passes
  - final_reports.top_passing_connections

columns:
  - name: finding_order
    type: INTEGER
    description: Display order for evidence-based finding.
    primary_key: true
  - name: evidence_area
    type: VARCHAR
    description: Analysis area supported by the finding.
  - name: finding
    type: VARCHAR
    description: Computed evidence statement.
  - name: caveat
    type: VARCHAR
    description: Scope and interpretation caveat.

@bruin */

WITH complete AS (
  SELECT COUNT(*) = 2 AND MIN(completed_matches) = 7 AS is_complete
  FROM `bruin-playground-arsalan.final_reports.team_kpis`
),
best_xg AS (
  SELECT *
  FROM `bruin-playground-arsalan.final_reports.team_kpis`
  QUALIFY ROW_NUMBER() OVER (ORDER BY xg_differential_per_match DESC, team_name) = 1
),
best_progression AS (
  SELECT *
  FROM `bruin-playground-arsalan.final_reports.progression_per_100_passes`
  QUALIFY ROW_NUMBER() OVER (ORDER BY completed_line_breaks_per_100_passes DESC, team_name) = 1
),
repeated_link AS (
  SELECT *
  FROM `bruin-playground-arsalan.final_reports.top_passing_connections`
  QUALIFY ROW_NUMBER() OVER (ORDER BY appearances_in_top_five DESC, mean_team_pass_share_pct DESC, team_name) = 1
)

SELECT 1 AS finding_order, 'Chance quality' AS evidence_area,
  FORMAT('%s comes out a touch ahead on xG differential: %+.2f xG per match across seven completed pre-final reports.', team_name, xg_differential_per_match) AS finding,
  'Seven matches are context, not a forecast of the final.' AS caveat
FROM best_xg CROSS JOIN complete
WHERE is_complete
UNION ALL
SELECT 2, 'Progression',
  FORMAT('%s completed %.1f line breaks per 100 passes, the higher rate in this seven-report sample.', team_name, completed_line_breaks_per_100_passes),
  'FIFA defines line breaks its own way, and the rate still moves with possession and opponents.'
FROM best_progression CROSS JOIN complete
WHERE is_complete
UNION ALL
SELECT 3, 'Passing connection',
  FORMAT('%s → %s kept showing up: FIFA listed it in the top five %d times, at a mean %.1f%% share of team passes when listed.', source_player, destination_player, appearances_in_top_five, mean_team_pass_share_pct),
  'FIFA publishes five links only, so this is not a complete passing network.'
FROM repeated_link CROSS JOIN complete
WHERE is_complete
ORDER BY finding_order
