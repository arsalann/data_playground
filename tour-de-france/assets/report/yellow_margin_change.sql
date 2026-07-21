/* @bruin
name: report.yellow_margin_change
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Measures how much the yellow-jersey lead over second place widened or tightened
  after each completed stage. Positive values widen the lead; negative values tighten it.

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
  - name: margin_to_second_seconds
    type: INTEGER
    description: GC gap from yellow to second place in seconds after the stage.
  - name: margin_change_seconds
    type: INTEGER
    description: Change in yellow's margin versus the previous completed stage; positive widens, negative tightens.
  - name: margin_change_direction
    type: VARCHAR
    description: Plain-language direction of margin movement after the stage.

unit_tests:
  - name: calculates_widening_and_tightening_of_yellow_margin
    inputs:
      - asset: staging.gc_standings
        rows:
          - {stage_number: 1, stage_date: '2026-07-04', stage_name: Barcelona, gc_rank: 2, gap_to_leader_seconds: 45, source_snapshot_id: stage_1, extracted_at: '2026-07-04T21:00:00Z'}
          - {stage_number: 2, stage_date: '2026-07-05', stage_name: Tarragona, gc_rank: 2, gap_to_leader_seconds: 80, source_snapshot_id: stage_2, extracted_at: '2026-07-05T21:00:00Z'}
          - {stage_number: 3, stage_date: '2026-07-06', stage_name: Les Angles, gc_rank: 2, gap_to_leader_seconds: 50, source_snapshot_id: stage_3, extracted_at: '2026-07-06T21:00:00Z'}
    expected:
      match: exact
      order: strict
      rows:
        - {stage_number: 1, margin_to_second_seconds: 45, margin_change_seconds: null, margin_change_direction: First completed stage}
        - {stage_number: 2, margin_to_second_seconds: 80, margin_change_seconds: 35, margin_change_direction: Widened}
        - {stage_number: 3, margin_to_second_seconds: 50, margin_change_seconds: -30, margin_change_direction: Tightened}

@bruin */

WITH deduped AS (
    SELECT *
    FROM staging.gc_standings
    WHERE gc_rank = 2
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY stage_number
        ORDER BY extracted_at DESC, source_snapshot_id DESC
    ) = 1
),
margins AS (
    SELECT
        stage_number,
        stage_date,
        FORMAT('Stage %d', stage_number) AS stage_label,
        gap_to_leader_seconds AS margin_to_second_seconds,
        gap_to_leader_seconds - LAG(gap_to_leader_seconds) OVER (
            ORDER BY stage_number
        ) AS margin_change_seconds
    FROM deduped
)

SELECT
    stage_number,
    stage_date,
    stage_label,
    margin_to_second_seconds,
    margin_change_seconds,
    CASE
        WHEN margin_change_seconds IS NULL THEN 'First completed stage'
        WHEN margin_change_seconds > 0 THEN 'Widened'
        WHEN margin_change_seconds < 0 THEN 'Tightened'
        ELSE 'Unchanged'
    END AS margin_change_direction
FROM margins
ORDER BY stage_number;
