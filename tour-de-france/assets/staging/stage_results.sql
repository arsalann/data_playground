/* @bruin
name: staging.stage_results
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Normalizes published stage-result payloads from the latest source snapshot.
  It parses placing and time-gap fields and applies a reproducible result-shape
  label; a candidate breakaway means the runner-up was more than 10 seconds behind.

depends:
  - staging.stage_latest

materialization:
  type: table
  strategy: create+replace

columns:
  - name: stage_number
    type: INTEGER
    description: Official 2026 Tour stage number.
    primary_key: true
    checks:
      - name: not_null
  - name: stage_date
    type: DATE
    description: Scheduled local race date for the stage.
  - name: stage_name
    type: VARCHAR
    description: Published route label for the stage.
  - name: stage_distance_km
    type: DOUBLE
    description: Published stage distance in kilometres.
  - name: stage_type
    type: VARCHAR
    description: Road-stage, team-time-trial, or individual-time-trial classification.
  - name: stage_rank
    type: INTEGER
    description: Published finishing rank within the stage result.
    primary_key: true
    checks:
      - name: not_null
  - name: rider_name
    type: VARCHAR
    description: Rider or winning team name as published by PCS.
  - name: rider_slug
    type: VARCHAR
    description: PCS rider slug when the result row belongs to an individual rider.
  - name: team_name
    type: VARCHAR
    description: Team name as published by PCS.
  - name: team_slug
    type: VARCHAR
    description: PCS team slug for the result row.
  - name: stage_time_text
    type: VARCHAR
    description: PCS result-time text; winner time for rank one and gap text for other placings.
  - name: stage_gap_seconds
    type: INTEGER
    description: Parsed gap to the stage winner in seconds when PCS supplies a time gap.
  - name: winner_gap_to_second_seconds
    type: INTEGER
    description: Runner-up gap in seconds, repeated per stage for transparent result-shape classification.
  - name: outcome_shape
    type: VARCHAR
    description: Time-trial, group/close finish, or candidate-breakaway label derived from published results.
  - name: source_snapshot_id
    type: VARCHAR
    description: Latest raw snapshot identifier from which this row was normalized.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC extraction time of the selected source snapshot.

custom_checks:
  - name: stage result keys are unique
    query: SELECT stage_number, stage_rank FROM staging.stage_results GROUP BY 1, 2 HAVING COUNT(*) != 1
    count: 0
  - name: parsed stage gaps are non-negative
    query: SELECT COUNT(*) FROM staging.stage_results WHERE stage_gap_seconds < 0
    value: 0

@bruin */

WITH latest_snapshot AS (
    SELECT *
    FROM staging.stage_latest
),
unnested_results AS (
    SELECT
        latest_snapshot.snapshot_id AS source_snapshot_id,
        latest_snapshot.stage_number,
        latest_snapshot.stage_date,
        latest_snapshot.stage_name,
        latest_snapshot.stage_distance_km,
        latest_snapshot.stage_type,
        latest_snapshot.extracted_at,
        SAFE_CAST(JSON_VALUE(result_json, '$.rank') AS INT64) AS stage_rank,
        NULLIF(JSON_VALUE(result_json, '$.rider_name'), '') AS rider_name,
        NULLIF(JSON_VALUE(result_json, '$.rider_slug'), '') AS rider_slug,
        NULLIF(JSON_VALUE(result_json, '$.team_name'), '') AS team_name,
        NULLIF(JSON_VALUE(result_json, '$.team_slug'), '') AS team_slug,
        NULLIF(JSON_VALUE(result_json, '$.stage_time'), '') AS stage_time_text
    FROM latest_snapshot
    CROSS JOIN UNNEST(JSON_QUERY_ARRAY(latest_snapshot.stage_results_payload)) AS result_json
),
parsed_gaps AS (
    SELECT
        *,
        CASE
            WHEN stage_rank = 1 THEN 0
            WHEN REGEXP_CONTAINS(stage_time_text, r'^\+?\d+:\d{2}:\d{2}$') THEN
                3600 * SAFE_CAST(REGEXP_EXTRACT(stage_time_text, r'^\+?(\d+):') AS INT64)
                + 60 * SAFE_CAST(REGEXP_EXTRACT(stage_time_text, r'^\+?\d+:(\d{2}):') AS INT64)
                + SAFE_CAST(REGEXP_EXTRACT(stage_time_text, r':(\d{2})$') AS INT64)
            WHEN REGEXP_CONTAINS(stage_time_text, r'^\+?\d+:\d{2}$') THEN
                60 * SAFE_CAST(REGEXP_EXTRACT(stage_time_text, r'^\+?(\d+):') AS INT64)
                + SAFE_CAST(REGEXP_EXTRACT(stage_time_text, r':(\d{2})$') AS INT64)
        END AS stage_gap_seconds
    FROM unnested_results
),
outcomes AS (
    SELECT
        *,
        MAX(IF(stage_rank = 2, stage_gap_seconds, NULL)) OVER (
            PARTITION BY source_snapshot_id
        ) AS winner_gap_to_second_seconds
    FROM parsed_gaps
)

SELECT
    stage_number,
    stage_date,
    stage_name,
    stage_distance_km,
    stage_type,
    stage_rank,
    rider_name,
    rider_slug,
    team_name,
    team_slug,
    stage_time_text,
    stage_gap_seconds,
    winner_gap_to_second_seconds,
    CASE
        WHEN stage_type IN ('Team time trial', 'Individual time trial') THEN stage_type
        WHEN winner_gap_to_second_seconds > 10 THEN 'Candidate breakaway (>10s)'
        ELSE 'Group/close finish (≤10s)'
    END AS outcome_shape,
    source_snapshot_id,
    extracted_at
FROM outcomes
WHERE stage_rank IS NOT NULL
ORDER BY stage_number, stage_rank;
