/* @bruin
name: staging.edc_h9_attrition
type: bq.sql
connection: bruin-playground-arsalan
description: |
  H9: "A large share of announced data centre megawatts is never built, so
  announcement and queue totals overstate future demand."

  Tested with the Compute Atlas statusHistory array, which records dated status
  transitions per facility. Facilities are grouped into cohorts by the year they
  first appear as proposed, and the cohort's current status mix shows how much
  converted to construction or operation.

  Two outputs are unioned:
  - cohort rows: conversion mix by first-proposed year
  - pipeline rows: total MW by current status, the scale of what is at stake

  MAJOR CAVEAT, and it cuts against the hypothesis: this is a survivorship-biased
  sample. Compute Atlas is a recent, actively curated dataset, so projects that
  were announced and quietly abandoned before curation began may never have been
  added at all. The cancelled share measured here is therefore a FLOOR, not an
  estimate. Cohorts are also right-censored: a 2026 proposal has had no time to
  become operational. Do not read the recent cohorts as attrition.

depends:
  - raw.edc_compute_atlas_facilities

materialization:
  type: table
  strategy: create+replace

columns:
  - name: metric_type
    type: VARCHAR
    description: "Either cohort or pipeline"
    primary_key: true
  - name: label
    type: VARCHAR
    description: First-proposed year for cohort rows, current status for pipeline rows
    primary_key: true
  - name: sort_order
    type: INTEGER
    description: Display order, ascending
  - name: facility_count
    type: INTEGER
    description: Facilities in this cohort or status
  - name: total_capacity_mw
    type: DOUBLE
    description: Sum of reported capacity in MW; only a minority of facilities report one
  - name: operational_pct
    type: DOUBLE
    description: Cohort rows only. Percentage now operational
  - name: under_construction_pct
    type: DOUBLE
    description: Cohort rows only. Percentage now under construction
  - name: still_proposed_pct
    type: DOUBLE
    description: Cohort rows only. Percentage still at proposed or permitted stage
  - name: cancelled_pct
    type: DOUBLE
    description: Cohort rows only. Percentage now cancelled; a floor, not an estimate
  - name: is_right_censored
    type: BOOLEAN
    description: True for cohorts too recent to have had time to convert

@bruin */

WITH facilities AS (
  SELECT
    facility_id,
    status,
    COALESCE(capacity_mw_operational, capacity_mw_planned) AS capacity_mw,
    status_history_json
  FROM `bruin-playground-arsalan.raw.edc_compute_atlas_facilities`
),

/* Earliest year recorded in the status history. Upstream dates are mixed
   granularity ("2026", "2026-03", "2026-03-05"), so take the leading 4 digits. */
first_seen AS (
  SELECT
    f.facility_id,
    f.status,
    f.capacity_mw,
    MIN(SAFE_CAST(REGEXP_EXTRACT(JSON_VALUE(entry, '$.date'), r'^(\d{4})') AS INT64)) AS first_year
  FROM facilities f,
    UNNEST(JSON_QUERY_ARRAY(f.status_history_json)) AS entry
  WHERE f.status_history_json IS NOT NULL
  GROUP BY f.facility_id, f.status, f.capacity_mw
),

cohorts AS (
  SELECT
    CAST(first_year AS STRING) AS label,
    first_year AS sort_order,
    COUNT(*) AS facility_count,
    SUM(capacity_mw) AS total_capacity_mw,
    ROUND(100 * COUNTIF(status = 'operational') / COUNT(*), 1) AS operational_pct,
    ROUND(100 * COUNTIF(status = 'under_construction') / COUNT(*), 1) AS under_construction_pct,
    ROUND(100 * COUNTIF(status IN ('proposed', 'permitted')) / COUNT(*), 1) AS still_proposed_pct,
    ROUND(100 * COUNTIF(status = 'cancelled') / COUNT(*), 1) AS cancelled_pct,
    first_year >= 2025 AS is_right_censored
  FROM first_seen
  WHERE first_year BETWEEN 2015 AND 2026
  GROUP BY first_year
),

pipeline AS (
  SELECT
    status AS label,
    CASE status
      WHEN 'operational' THEN 1
      WHEN 'under_construction' THEN 2
      WHEN 'permitted' THEN 3
      WHEN 'proposed' THEN 4
      WHEN 'cancelled' THEN 5
      ELSE 9
    END AS sort_order,
    COUNT(*) AS facility_count,
    SUM(capacity_mw) AS total_capacity_mw
  FROM facilities
  WHERE status IS NOT NULL
  GROUP BY status
)

SELECT
  'cohort' AS metric_type,
  label,
  sort_order,
  facility_count,
  ROUND(total_capacity_mw, 1) AS total_capacity_mw,
  operational_pct,
  under_construction_pct,
  still_proposed_pct,
  cancelled_pct,
  is_right_censored
FROM cohorts

UNION ALL

SELECT
  'pipeline',
  label,
  sort_order,
  facility_count,
  ROUND(total_capacity_mw, 1),
  CAST(NULL AS FLOAT64),
  CAST(NULL AS FLOAT64),
  CAST(NULL AS FLOAT64),
  CAST(NULL AS FLOAT64),
  FALSE
FROM pipeline

ORDER BY metric_type, sort_order
