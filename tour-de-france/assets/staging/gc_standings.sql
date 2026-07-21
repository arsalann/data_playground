/* @bruin
name: staging.gc_standings
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Normalizes the top 30 general-classification standings published after each
  completed stage. It parses gaps to the yellow jersey and calculates rank
  movement from the PCS prior-rank field.

depends:
  - staging.stage_latest

materialization:
  type: table
  strategy: create+replace

columns:
  - name: stage_number
    type: INTEGER
    description: Official stage number after which the GC classification applies.
    primary_key: true
    checks:
      - name: not_null
  - name: stage_date
    type: DATE
    description: Scheduled local race date for the completed stage.
  - name: stage_name
    type: VARCHAR
    description: Route label for the stage after which the GC applies.
  - name: gc_rank
    type: INTEGER
    description: Published GC rank after the stage.
    primary_key: true
    checks:
      - name: not_null
  - name: previous_gc_rank
    type: INTEGER
    description: PCS rank before this stage when published.
  - name: rank_movement
    type: INTEGER
    description: Positions gained since the previous stage; positive values mean a rider moved up.
  - name: rider_name
    type: VARCHAR
    description: Rider name as published by PCS.
    checks:
      - name: not_null
  - name: rider_slug
    type: VARCHAR
    description: PCS rider slug.
  - name: team_name
    type: VARCHAR
    description: Team name as published by PCS.
  - name: team_slug
    type: VARCHAR
    description: PCS team slug.
  - name: gc_time_text
    type: VARCHAR
    description: PCS GC time for the leader or gap text for other ranks.
  - name: gap_to_leader_seconds
    type: INTEGER
    description: Parsed gap to the yellow jersey in seconds; zero for the leader.
    checks:
      - name: not_null
  - name: source_snapshot_id
    type: VARCHAR
    description: Latest raw snapshot identifier from which this standing was normalized.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC extraction time of the selected source snapshot.

custom_checks:
  - name: GC keys are unique
    query: SELECT stage_number, gc_rank FROM staging.gc_standings GROUP BY 1, 2 HAVING COUNT(*) != 1
    count: 0
  - name: GC gaps are non-negative
    query: SELECT COUNT(*) FROM staging.gc_standings WHERE gap_to_leader_seconds < 0
    value: 0
  - name: every completed stage has a top-30 snapshot
    query: |
      SELECT stage_number
      FROM staging.gc_standings
      GROUP BY stage_number
      HAVING COUNT(*) < 30
    count: 0

@bruin */

WITH latest_snapshot AS (
    SELECT *
    FROM staging.stage_latest
),
unnested_gc AS (
    SELECT
        latest_snapshot.snapshot_id AS source_snapshot_id,
        latest_snapshot.stage_number,
        latest_snapshot.stage_date,
        latest_snapshot.stage_name,
        latest_snapshot.extracted_at,
        SAFE_CAST(JSON_VALUE(gc_json, '$.rank') AS INT64) AS gc_rank,
        SAFE_CAST(JSON_VALUE(gc_json, '$.previous_rank') AS INT64) AS previous_gc_rank,
        NULLIF(JSON_VALUE(gc_json, '$.rider_name'), '') AS rider_name,
        NULLIF(JSON_VALUE(gc_json, '$.rider_slug'), '') AS rider_slug,
        NULLIF(JSON_VALUE(gc_json, '$.team_name'), '') AS team_name,
        NULLIF(JSON_VALUE(gc_json, '$.team_slug'), '') AS team_slug,
        NULLIF(JSON_VALUE(gc_json, '$.gc_time'), '') AS gc_time_text
    FROM latest_snapshot
    CROSS JOIN UNNEST(JSON_QUERY_ARRAY(latest_snapshot.gc_top30_payload)) AS gc_json
),
parsed_gaps AS (
    SELECT
        *,
        CASE
            WHEN gc_rank = 1 THEN 0
            WHEN REGEXP_CONTAINS(gc_time_text, r'^\+?\d+:\d{2}:\d{2}$') THEN
                3600 * SAFE_CAST(REGEXP_EXTRACT(gc_time_text, r'^\+?(\d+):') AS INT64)
                + 60 * SAFE_CAST(REGEXP_EXTRACT(gc_time_text, r'^\+?\d+:(\d{2}):') AS INT64)
                + SAFE_CAST(REGEXP_EXTRACT(gc_time_text, r':(\d{2})$') AS INT64)
            WHEN REGEXP_CONTAINS(gc_time_text, r'^\+?\d+:\d{2}$') THEN
                60 * SAFE_CAST(REGEXP_EXTRACT(gc_time_text, r'^\+?(\d+):') AS INT64)
                + SAFE_CAST(REGEXP_EXTRACT(gc_time_text, r':(\d{2})$') AS INT64)
        END AS gap_to_leader_seconds
    FROM unnested_gc
)

SELECT
    stage_number,
    stage_date,
    stage_name,
    gc_rank,
    previous_gc_rank,
    previous_gc_rank - gc_rank AS rank_movement,
    rider_name,
    rider_slug,
    team_name,
    team_slug,
    gc_time_text,
    COALESCE(gap_to_leader_seconds, 0) AS gap_to_leader_seconds,
    source_snapshot_id,
    extracted_at
FROM parsed_gaps
WHERE gc_rank BETWEEN 1 AND 30
ORDER BY stage_number, gc_rank;
