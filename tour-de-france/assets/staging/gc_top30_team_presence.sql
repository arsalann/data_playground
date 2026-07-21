/* @bruin
name: staging.gc_top30_team_presence
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Aggregates team representation in the GC top 30 after each completed stage.

depends:
  - staging.gc_standings

materialization:
  type: table
  strategy: create+replace

columns:
  - name: stage_number
    type: INTEGER
    description: Official stage number after which the GC applies.
    primary_key: true
  - name: team_name
    type: VARCHAR
    description: Team represented in the GC top 30.
    primary_key: true
  - name: riders_in_top30
    type: INTEGER
    description: Number of riders from the team in the GC top 30.
  - name: best_gc_rank
    type: INTEGER
    description: Best, lowest-numbered GC rank held by the team.

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
    team_name,
    COUNT(*) AS riders_in_top30,
    MIN(gc_rank) AS best_gc_rank
FROM deduped
WHERE team_name IS NOT NULL
GROUP BY stage_number, team_name
ORDER BY stage_number, best_gc_rank, team_name;
