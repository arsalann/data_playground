/* @bruin
name: report.gc_gaps_by_stage
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Pivots post-stage GC gaps at second, third, and fifth place for the yellow
  jersey race-dynamics line chart.

depends:
  - staging.gc_standings

materialization:
  type: table
  strategy: create+replace

columns:
  - name: stage_number
    type: INTEGER
    description: Official completed stage number.
    primary_key: true
  - name: stage_date
    type: DATE
    description: Scheduled local date for the completed stage.
  - name: stage_label
    type: VARCHAR
    description: Readable completed-stage label for chart and tooltip use.
  - name: gap_rank_2_seconds
    type: INTEGER
    description: GC gap to second place in seconds after the stage.
  - name: gap_rank_3_seconds
    type: INTEGER
    description: GC gap to third place in seconds after the stage.
  - name: gap_rank_5_seconds
    type: INTEGER
    description: GC gap to fifth place in seconds after the stage.

@bruin */

WITH deduped AS (
    SELECT *
    FROM staging.gc_standings
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY stage_number, gc_rank
        ORDER BY extracted_at DESC, source_snapshot_id DESC
    ) = 1
)

SELECT
    stage_number,
    ANY_VALUE(stage_date) AS stage_date,
    FORMAT('Stage %d', stage_number) AS stage_label,
    MAX(IF(gc_rank = 2, gap_to_leader_seconds, NULL)) AS gap_rank_2_seconds,
    MAX(IF(gc_rank = 3, gap_to_leader_seconds, NULL)) AS gap_rank_3_seconds,
    MAX(IF(gc_rank = 5, gap_to_leader_seconds, NULL)) AS gap_rank_5_seconds
FROM deduped
GROUP BY stage_number
ORDER BY stage_number;
