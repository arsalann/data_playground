/* @bruin
name: report.race_kpis
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Produces one current-race KPI row: completed stages, completed distance, the
  latest yellow-jersey margin to second, and unique stage winners.

depends:
  - staging.stage_results
  - staging.gc_standings

materialization:
  type: table
  strategy: create+replace

columns:
  - name: completed_stages
    type: INTEGER
    description: Count of stages with a published latest snapshot.
  - name: completed_distance_km
    type: DOUBLE
    description: Sum of published completed-stage distances in kilometres.
  - name: gc_margin_to_second_seconds
    type: INTEGER
    description: Latest GC gap from yellow to second place in seconds.
  - name: unique_stage_winners
    type: INTEGER
    description: Count of distinct rider or team names winning completed stages.
  - name: latest_stage_number
    type: INTEGER
    description: Most recently completed official stage number.
  - name: latest_stage_date
    type: DATE
    description: Scheduled local date of the latest completed stage.

custom_checks:
  - name: exactly one KPI row
    query: SELECT COUNT(*) FROM report.race_kpis
    value: 1

@bruin */

WITH completed_stages AS (
    SELECT
        stage_number,
        ANY_VALUE(stage_date) AS stage_date,
        ANY_VALUE(stage_distance_km) AS stage_distance_km
    FROM staging.stage_results
    GROUP BY stage_number
),
stage_winners AS (
    SELECT rider_name
    FROM staging.stage_results
    WHERE stage_rank = 1
),
latest_stage AS (
    SELECT AS STRUCT stage_number, stage_date
    FROM completed_stages
    ORDER BY stage_number DESC
    LIMIT 1
),
latest_gc_margin AS (
    SELECT gc_standings.gap_to_leader_seconds
    FROM staging.gc_standings AS gc_standings
    CROSS JOIN latest_stage
    WHERE gc_standings.stage_number = latest_stage.stage_number
      AND gc_standings.gc_rank = 2
)

SELECT
    COUNT(*) AS completed_stages,
    ROUND(COALESCE(SUM(stage_distance_km), 0), 1) AS completed_distance_km,
    COALESCE((SELECT gap_to_leader_seconds FROM latest_gc_margin), 0) AS gc_margin_to_second_seconds,
    (SELECT COUNT(DISTINCT rider_name) FROM stage_winners WHERE rider_name IS NOT NULL) AS unique_stage_winners,
    (SELECT stage_number FROM latest_stage) AS latest_stage_number,
    (SELECT stage_date FROM latest_stage) AS latest_stage_date
FROM completed_stages;
