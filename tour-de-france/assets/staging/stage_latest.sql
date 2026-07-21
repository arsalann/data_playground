/* @bruin
name: staging.stage_latest
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Selects the newest complete ProCyclingStats source snapshot for each published
  2026 Tour stage. Raw snapshot history remains intact for source-revision audit.

depends:
  - raw.stage_snapshots

materialization:
  type: table
  strategy: create+replace

columns:
  - name: snapshot_id
    type: VARCHAR
    description: Immutable identifier of the newest source snapshot for this stage.
    primary_key: true
    checks:
      - name: unique
      - name: not_null
  - name: stage_number
    type: INTEGER
    description: Official 2026 Tour stage number.
    checks:
      - name: not_null
  - name: stage_date
    type: DATE
    description: Scheduled local race date for the completed stage.
  - name: stage_name
    type: VARCHAR
    description: Published route label for the stage.
  - name: stage_distance_km
    type: DOUBLE
    description: Published stage distance in kilometres.
  - name: stage_type
    type: VARCHAR
    description: Road-stage, team-time-trial, or individual-time-trial classification.
  - name: stage_status
    type: VARCHAR
    description: Source publication status for the selected snapshot.
  - name: stage_results_payload
    type: VARCHAR
    description: Latest complete stage-result JSON payload.
  - name: gc_top30_payload
    type: VARCHAR
    description: Latest complete top-30 post-stage GC JSON payload.
  - name: stage_source_url
    type: VARCHAR
    description: PCS URL used for the stage results.
  - name: gc_source_url
    type: VARCHAR
    description: PCS URL used for the post-stage GC.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp at which the selected source snapshot was captured.

custom_checks:
  - name: one latest row per published stage
    query: SELECT stage_number FROM staging.stage_latest GROUP BY stage_number HAVING COUNT(*) != 1
    count: 0
  - name: latest snapshot coverage
    description: No newer published raw snapshot may be absent from this latest-snapshot table.
    query: |
      SELECT COUNT(*)
      FROM raw.stage_snapshots AS raw_snapshot
      LEFT JOIN staging.stage_latest AS latest_snapshot
        USING (stage_number)
      WHERE raw_snapshot.stage_status = 'published'
        AND raw_snapshot.extracted_at > latest_snapshot.extracted_at
    value: 0

unit_tests:
  - name: keeps_only_the_newest_snapshot_per_stage
    inputs:
      - asset: raw.stage_snapshots
        rows:
          - {snapshot_id: stage_1_old, stage_number: 1, stage_date: '2026-07-04', stage_name: Barcelona, stage_distance_km: 19.6, stage_type: Team time trial, stage_status: published, stage_results_payload: '[]', gc_top30_payload: '[]', stage_source_url: https://example.test/stage-1, gc_source_url: https://example.test/stage-1-gc, extracted_at: '2026-07-04T20:00:00Z'}
          - {snapshot_id: stage_1_new, stage_number: 1, stage_date: '2026-07-04', stage_name: Barcelona, stage_distance_km: 19.6, stage_type: Team time trial, stage_status: published, stage_results_payload: '[]', gc_top30_payload: '[]', stage_source_url: https://example.test/stage-1, gc_source_url: https://example.test/stage-1-gc, extracted_at: '2026-07-04T21:00:00Z'}
          - {snapshot_id: stage_2_only, stage_number: 2, stage_date: '2026-07-05', stage_name: Tarragona, stage_distance_km: 168.5, stage_type: Road stage, stage_status: published, stage_results_payload: '[]', gc_top30_payload: '[]', stage_source_url: https://example.test/stage-2, gc_source_url: https://example.test/stage-2-gc, extracted_at: '2026-07-05T21:00:00Z'}
    expected:
      count: 2
      match: exact
      rows:
        - {snapshot_id: stage_1_new, stage_number: 1}
        - {snapshot_id: stage_2_only, stage_number: 2}

@bruin */

WITH latest_snapshot AS (
    SELECT
        *
    FROM raw.stage_snapshots
    WHERE stage_status = 'published'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY stage_number
        ORDER BY extracted_at DESC, snapshot_id DESC
    ) = 1
)

SELECT
    snapshot_id,
    stage_number,
    stage_date,
    stage_name,
    stage_distance_km,
    stage_type,
    stage_status,
    stage_results_payload,
    gc_top30_payload,
    stage_source_url,
    gc_source_url,
    extracted_at
FROM latest_snapshot
ORDER BY stage_number;
