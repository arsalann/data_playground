/* @bruin
name: report.current_gc_top10
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Returns the latest published top 10 in general classification for the live
  dashboard table, including rank movement and gaps to yellow.

depends:
  - staging.gc_standings

materialization:
  type: table
  strategy: create+replace

columns:
  - name: gc_rank
    type: INTEGER
    description: Current published GC rank.
    primary_key: true
  - name: rider_name
    type: VARCHAR
    description: Rider name as published by PCS.
  - name: team_name
    type: VARCHAR
    description: Rider team as published by PCS.
  - name: gap_to_leader_seconds
    type: INTEGER
    description: Gap to yellow in seconds.
  - name: rank_movement
    type: INTEGER
    description: Positions gained since the prior stage; positive means moved up.
  - name: latest_stage_number
    type: INTEGER
    description: Stage number defining this latest GC classification.
  - name: latest_stage_date
    type: DATE
    description: Scheduled local date defining this latest GC classification.

@bruin */

WITH latest_stage AS (
    SELECT MAX(stage_number) AS stage_number
    FROM staging.gc_standings
),
deduped AS (
    SELECT gc_standings.*
    FROM staging.gc_standings
    CROSS JOIN latest_stage
    WHERE gc_standings.stage_number = latest_stage.stage_number
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY gc_standings.gc_rank
        ORDER BY gc_standings.extracted_at DESC, gc_standings.source_snapshot_id DESC
    ) = 1
)

SELECT
    gc_rank,
    rider_name,
    team_name,
    gap_to_leader_seconds,
    rank_movement,
    stage_number AS latest_stage_number,
    stage_date AS latest_stage_date
FROM deduped
WHERE gc_rank <= 10
ORDER BY gc_rank;
