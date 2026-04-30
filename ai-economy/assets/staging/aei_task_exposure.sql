/* @bruin

name: staging.aei_task_exposure
type: bq.sql
connection: bruin-playground-arsalan
description: |
  One row per AEI O*NET task (current 2026-01-15 release, GLOBAL geography),
  wide-pivoted with the metrics needed for the augmentation-vs-automation chart.

  Joins AEI tasks to O*NET by lowercased task description; joins O*NET-SOC (8-digit)
  to BLS SOC (6-digit root) for US wage and employment enrichment.

depends:
  - raw.aei_claude_usage
  - raw.aei_onet_tasks
  - raw.aei_bls_wages

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

global_onet AS (
    SELECT
        LOWER(TRIM(cluster_name)) AS task_text,
        variable,
        value
    FROM deduped
    WHERE geo_id = 'GLOBAL'
      AND facet = 'onet_task'
      AND cluster_name != ''
      AND LOWER(TRIM(cluster_name)) NOT IN ('not_classified', 'none', 'unclassified')
),

base_pivot AS (
    SELECT
        task_text,
        MAX(IF(variable = 'onet_task_count', value, NULL)) AS usage_count_global,
        MAX(IF(variable = 'onet_task_pct',   value, NULL)) AS usage_pct_global
    FROM global_onet
    GROUP BY task_text
),

global_ai_autonomy AS (
    SELECT
        LOWER(TRIM(cluster_name)) AS task_text,
        MAX(IF(variable = 'onet_task_ai_autonomy_mean', value, NULL)) AS ai_autonomy_mean
    FROM deduped
    WHERE geo_id = 'GLOBAL' AND facet = 'onet_task::ai_autonomy'
    GROUP BY task_text
),

global_times AS (
    SELECT
        LOWER(TRIM(cluster_name)) AS task_text,
        MAX(IF(facet = 'onet_task::human_only_time'      AND variable = 'onet_task_human_only_time_mean',      value, NULL)) AS human_only_time_mean,
        MAX(IF(facet = 'onet_task::human_with_ai_time'   AND variable = 'onet_task_human_with_ai_time_mean',   value, NULL)) AS human_with_ai_time_mean,
        MAX(IF(facet = 'onet_task::human_education_years' AND variable = 'onet_task_human_education_years_mean', value, NULL)) AS human_education_years_mean
    FROM deduped
    WHERE geo_id = 'GLOBAL'
      AND facet IN ('onet_task::human_only_time', 'onet_task::human_with_ai_time', 'onet_task::human_education_years')
    GROUP BY task_text
),

-- Collaboration intersection uses cluster_name = "<task_text>::<collab_type>".
collab_parsed AS (
    SELECT
        LOWER(TRIM(REGEXP_REPLACE(cluster_name, r'::[^:]+$', ''))) AS task_text,
        REGEXP_EXTRACT(cluster_name, r'::([^:]+)$')                AS collab_type,
        value                                                      AS share_pct
    FROM deduped
    WHERE geo_id = 'GLOBAL'
      AND facet = 'onet_task::collaboration'
      AND variable = 'onet_task_collaboration_pct'
),

collab_pivot AS (
    SELECT
        task_text,
        MAX(IF(collab_type = 'directive',       share_pct, NULL)) AS collab_directive_pct,
        MAX(IF(collab_type = 'task iteration',  share_pct, NULL)) AS collab_task_iteration_pct,
        MAX(IF(collab_type = 'feedback loop',   share_pct, NULL)) AS collab_feedback_loop_pct,
        MAX(IF(collab_type = 'validation',      share_pct, NULL)) AS collab_validation_pct,
        MAX(IF(collab_type = 'learning',        share_pct, NULL)) AS collab_learning_pct,
        MAX(IF(collab_type = 'none',            share_pct, NULL)) AS collab_none_pct
    FROM collab_parsed
    GROUP BY task_text
),

onet_lookup AS (
    SELECT
        task_description_lower AS task_text,
        ANY_VALUE(task_id)              AS task_id,
        ANY_VALUE(onet_soc_code)        AS onet_soc_code,
        ANY_VALUE(occupation_title)     AS occupation_title,
        ANY_VALUE(task_description)     AS task_description,
        ANY_VALUE(task_type)            AS task_type
    FROM `bruin-playground-arsalan.raw.aei_onet_tasks`
    WHERE task_description_lower IS NOT NULL
    GROUP BY task_description_lower
),

bls_lookup AS (
    SELECT
        soc_code,
        total_employment,
        median_annual_wage,
        mean_annual_wage
    FROM `bruin-playground-arsalan.raw.aei_bls_wages`
)

SELECT
    p.task_text,
    o.task_id,
    o.task_description,
    o.onet_soc_code,
    -- SOC alignment: O*NET "15-1252.00" → BLS "15-1252"
    REGEXP_EXTRACT(o.onet_soc_code, r'^\d{2}-\d{4}') AS soc_code_6digit,
    o.occupation_title,
    o.task_type,
    p.usage_count_global,
    p.usage_pct_global,
    a.ai_autonomy_mean,
    t.human_only_time_mean,
    t.human_with_ai_time_mean,
    t.human_education_years_mean,
    c.collab_directive_pct,
    c.collab_task_iteration_pct,
    c.collab_feedback_loop_pct,
    c.collab_validation_pct,
    c.collab_learning_pct,
    c.collab_none_pct,
    b.total_employment   AS bls_total_employment,
    b.median_annual_wage AS bls_median_annual_wage,
    b.mean_annual_wage   AS bls_mean_annual_wage,
    -- Exposure pattern bucket. AEI ai_autonomy_mean is on a 1-5 scale where 1=directive
    -- (heavy human control) and 5=fully delegated. Bucket boundaries are empirical
    -- based on AEI docs (1-2.5 = augmentation, 2.5-3.5 = hybrid, 3.5-5 = automation).
    CASE
        WHEN a.ai_autonomy_mean IS NULL THEN NULL
        WHEN a.ai_autonomy_mean < 2.5 THEN 'Augmentation'
        WHEN a.ai_autonomy_mean < 3.5 THEN 'Hybrid'
        ELSE 'Automation'
    END AS exposure_pattern,
    -- Time-saving ratio: how much faster does a human become with Claude on this task?
    SAFE_DIVIDE(t.human_only_time_mean, t.human_with_ai_time_mean) AS time_speedup_ratio
FROM base_pivot p
LEFT JOIN onet_lookup        o USING (task_text)
LEFT JOIN global_ai_autonomy a USING (task_text)
LEFT JOIN global_times       t USING (task_text)
LEFT JOIN collab_pivot       c USING (task_text)
LEFT JOIN bls_lookup         b ON b.soc_code = REGEXP_EXTRACT(o.onet_soc_code, r'^\d{2}-\d{4}')
ORDER BY p.usage_count_global DESC
