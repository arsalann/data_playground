/* @bruin

name: staging.aei_collaboration_patterns
type: bq.sql
connection: bruin-playground-arsalan
description: |
  One row per (O*NET task, collaboration_type) with the share of Claude.ai conversations
  on that task that followed the given pattern. Shares within a task sum to ~100%
  (minor float rounding allowed; custom_check asserts ≤1 pp divergence).

  Source: `onet_task::collaboration` intersection facet from the current release.
  Collaboration types observed: directive, task iteration, feedback loop, learning,
  validation, none, not_classified.

depends:
  - raw.aei_claude_usage
  - raw.aei_onet_tasks

materialization:
  type: table
  strategy: create+replace

@bruin */

WITH deduped AS (
    SELECT *
    FROM `bruin-playground-arsalan.raw.aei_claude_usage`
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY geo_id, date_start, facet, variable, cluster_name
        ORDER BY extracted_at DESC
    ) = 1
),

raw_collab AS (
    SELECT
        LOWER(TRIM(REGEXP_REPLACE(cluster_name, r'::[^:]+$', ''))) AS task_text,
        REGEXP_EXTRACT(cluster_name, r'::([^:]+)$')                AS collaboration_type,
        value                                                      AS share_pct
    FROM deduped
    WHERE geo_id = 'GLOBAL'
      AND facet = 'onet_task::collaboration'
      AND variable = 'onet_task_collaboration_pct'
      AND cluster_name LIKE '%::%'
),

-- Per-task usage count for downstream filtering (top-20 in chart).
task_volume AS (
    SELECT
        LOWER(TRIM(cluster_name)) AS task_text,
        MAX(IF(variable = 'onet_task_count', value, NULL)) AS task_usage_count_global
    FROM deduped
    WHERE geo_id = 'GLOBAL'
      AND facet = 'onet_task'
      AND cluster_name != ''
    GROUP BY task_text
),

onet_lookup AS (
    SELECT
        task_description_lower AS task_text,
        ANY_VALUE(task_id)           AS task_id,
        ANY_VALUE(task_description)  AS task_description,
        ANY_VALUE(onet_soc_code)     AS onet_soc_code,
        ANY_VALUE(occupation_title)  AS occupation_title
    FROM `bruin-playground-arsalan.raw.aei_onet_tasks`
    WHERE task_description_lower IS NOT NULL
    GROUP BY task_description_lower
)

SELECT
    r.task_text,
    r.collaboration_type,
    r.share_pct,
    v.task_usage_count_global,
    o.task_id,
    o.task_description,
    o.onet_soc_code,
    o.occupation_title
FROM raw_collab r
LEFT JOIN task_volume v USING (task_text)
LEFT JOIN onet_lookup o USING (task_text)
WHERE r.collaboration_type IS NOT NULL
ORDER BY v.task_usage_count_global DESC, r.task_text, r.collaboration_type
