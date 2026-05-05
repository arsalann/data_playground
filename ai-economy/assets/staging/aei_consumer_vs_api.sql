/* @bruin

name: staging.aei_consumer_vs_api
type: bq.sql
connection: bruin-playground-arsalan
description: |
  One row per O*NET task comparing Claude.ai (consumer) and 1P API (developer/enterprise)
  global usage shares, with API cost and token indices.

  Both sides are GLOBAL-only — 1P API is not broken down by country. Inner join on
  lowercased task description drops tasks present in only one channel.

  `delta_pp` = api_pct - consumer_pct (positive = enterprise-skewed).

depends:
  - raw.aei_claude_usage
  - raw.aei_api_usage
  - raw.aei_onet_tasks

materialization:
  type: table
  strategy: create+replace

@bruin */

WITH claude_deduped AS (
    SELECT *
    FROM `bruin-playground-arsalan.raw.aei_claude_usage`
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY geo_id, date_start, facet, variable, cluster_name
        ORDER BY extracted_at DESC
    ) = 1
),

api_deduped AS (
    SELECT *
    FROM `bruin-playground-arsalan.raw.aei_api_usage`
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY geo_id, date_start, facet, variable, cluster_name
        ORDER BY extracted_at DESC
    ) = 1
),

consumer AS (
    SELECT
        LOWER(TRIM(cluster_name)) AS task_text,
        MAX(IF(variable = 'onet_task_pct',   value, NULL)) AS consumer_pct,
        MAX(IF(variable = 'onet_task_count', value, NULL)) AS consumer_count
    FROM claude_deduped
    WHERE geo_id = 'GLOBAL'
      AND facet = 'onet_task'
      AND cluster_name != ''
      AND LOWER(TRIM(cluster_name)) NOT IN ('not_classified', 'none', 'unclassified')
    GROUP BY task_text
),

api_share AS (
    SELECT
        LOWER(TRIM(cluster_name)) AS task_text,
        MAX(IF(variable = 'onet_task_pct',   value, NULL)) AS api_pct,
        MAX(IF(variable = 'onet_task_count', value, NULL)) AS api_count
    FROM api_deduped
    WHERE geo_id = 'GLOBAL'
      AND facet = 'onet_task'
      AND cluster_name != ''
      AND LOWER(TRIM(cluster_name)) NOT IN ('not_classified', 'none', 'unclassified')
    GROUP BY task_text
),

-- Cost / token cluster_names are suffixed `::value`; strip it so the join key matches
-- the bare task text used elsewhere in this asset.
api_cost AS (
    SELECT
        LOWER(TRIM(REGEXP_REPLACE(cluster_name, r'::[^:]+$', ''))) AS task_text,
        MAX(IF(facet = 'onet_task::cost'              AND variable = 'cost_index',              value, NULL)) AS api_cost_index,
        MAX(IF(facet = 'onet_task::prompt_tokens'     AND variable = 'prompt_tokens_index',     value, NULL)) AS api_prompt_tokens_index,
        MAX(IF(facet = 'onet_task::completion_tokens' AND variable = 'completion_tokens_index', value, NULL)) AS api_completion_tokens_index
    FROM api_deduped
    WHERE geo_id = 'GLOBAL'
      AND facet IN ('onet_task::cost', 'onet_task::prompt_tokens', 'onet_task::completion_tokens')
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
    c.task_text,
    o.task_id,
    o.task_description,
    o.onet_soc_code,
    o.occupation_title,
    c.consumer_pct,
    a.api_pct,
    (a.api_pct - c.consumer_pct) AS delta_pp,
    c.consumer_count,
    a.api_count,
    x.api_cost_index,
    x.api_prompt_tokens_index,
    x.api_completion_tokens_index
FROM consumer c
INNER JOIN api_share a USING (task_text)
LEFT  JOIN api_cost  x USING (task_text)
LEFT  JOIN onet_lookup o USING (task_text)
ORDER BY ABS(a.api_pct - c.consumer_pct) DESC
