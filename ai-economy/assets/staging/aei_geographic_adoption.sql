/* @bruin

name: staging.aei_geographic_adoption
type: bq.sql
connection: bruin-playground-arsalan
description: |
  One row per (country × release). Country-level AEI usage joined to World Bank GDP,
  labor, and population indicators, with a specialization-index column identifying
  each country's signature task.

  Releases included: 2026-01-15 (current) and 2025-09-15 (prior), both from the
  Claude.ai platform. 1P API data is NOT broken down by country.

  Country codes are normalized to ISO alpha-3 using `raw.aei_iso_country_codes`.
  World Bank indicators use the max year available per country (2020-2024 window).

  Filters: `usage_count >= 200` (AEI publication threshold for country-level rows).

depends:
  - raw.aei_claude_usage
  - raw.aei_prior_snapshot
  - raw.aei_worldbank_context
  - raw.aei_iso_country_codes

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

prior_deduped AS (
    SELECT *
    FROM `bruin-playground-arsalan.raw.aei_prior_snapshot`
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY geo_id, date_start, facet, variable, cluster_name
        ORDER BY extracted_at DESC
    ) = 1
),

combined AS (
    SELECT release_id, geo_id, geography, facet, variable, cluster_name, value FROM claude_deduped
    UNION ALL
    SELECT release_id, geo_id, geography, facet, variable, cluster_name, value FROM prior_deduped
),

-- Country-level usage counts (facet = 'country', variable = 'usage_count').
country_totals AS (
    SELECT
        release_id,
        geo_id AS iso_alpha_2,
        MAX(IF(variable = 'usage_count', value, NULL)) AS usage_count,
        MAX(IF(variable = 'usage_pct',   value, NULL)) AS usage_pct_of_global
    FROM combined
    WHERE geography = 'country'
      AND facet = 'country'
    GROUP BY release_id, iso_alpha_2
),

-- Per-country, per-task share — used to compute specialization vs global baseline.
-- Filter out AEI's catch-all cluster names (`not_classified`, `none`) which are not real
-- O*NET tasks and would dominate any country's "top specialization" when included.
country_task AS (
    SELECT
        release_id,
        geo_id AS iso_alpha_2,
        LOWER(TRIM(cluster_name)) AS task_text,
        value AS country_task_pct
    FROM combined
    WHERE geography = 'country'
      AND facet = 'onet_task'
      AND variable = 'onet_task_pct'
      AND cluster_name != ''
      AND LOWER(TRIM(cluster_name)) NOT IN ('not_classified', 'none', 'unclassified')
),

global_task AS (
    SELECT
        release_id,
        LOWER(TRIM(cluster_name)) AS task_text,
        value AS global_task_pct
    FROM combined
    WHERE geo_id = 'GLOBAL'
      AND facet = 'onet_task'
      AND variable = 'onet_task_pct'
      AND cluster_name != ''
      AND LOWER(TRIM(cluster_name)) NOT IN ('not_classified', 'none', 'unclassified')
),

-- Specialization index = country share / global share. > 1 = over-indexed on this task.
specialization AS (
    SELECT
        ct.release_id,
        ct.iso_alpha_2,
        ct.task_text,
        ct.country_task_pct,
        g.global_task_pct,
        SAFE_DIVIDE(ct.country_task_pct, g.global_task_pct) AS specialization_index
    FROM country_task ct
    LEFT JOIN global_task g
      ON g.release_id = ct.release_id AND g.task_text = ct.task_text
),

-- Each country's top task by specialization_index (require min 1% country-share to avoid noise).
top_task AS (
    SELECT
        release_id,
        iso_alpha_2,
        task_text AS top_task_text,
        specialization_index AS top_task_specialization_index,
        country_task_pct AS top_task_country_pct
    FROM specialization
    WHERE specialization_index IS NOT NULL
      AND country_task_pct >= 1.0
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY release_id, iso_alpha_2
        ORDER BY specialization_index DESC
    ) = 1
),

distinct_task_count AS (
    SELECT
        release_id,
        iso_alpha_2,
        COUNT(DISTINCT task_text) AS distinct_tasks_observed
    FROM country_task
    GROUP BY release_id, iso_alpha_2
),

iso_map AS (
    SELECT
        iso_alpha_2,
        ANY_VALUE(iso_alpha_3)  AS iso_alpha_3,
        ANY_VALUE(country_name) AS iso_country_name
    FROM `bruin-playground-arsalan.raw.aei_iso_country_codes`
    WHERE iso_alpha_2 IS NOT NULL
    GROUP BY iso_alpha_2
),

-- World Bank: pick the latest non-null value per (country, indicator) in the 2020-2024 window.
wb_deduped AS (
    SELECT *
    FROM `bruin-playground-arsalan.raw.aei_worldbank_context`
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY country_code, indicator_code, year
        ORDER BY extracted_at DESC
    ) = 1
),

wb_latest AS (
    SELECT
        country_code AS iso_alpha_3,
        indicator_code,
        value
    FROM wb_deduped
    WHERE country_code IS NOT NULL
      AND country_code != ''
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY country_code, indicator_code
        ORDER BY year DESC, extracted_at DESC
    ) = 1
),

wb_pivot AS (
    SELECT
        iso_alpha_3,
        MAX(IF(indicator_code = 'NY.GDP.PCAP.CD', value, NULL)) AS gdp_per_capita,
        MAX(IF(indicator_code = 'SL.TLF.CACT.ZS', value, NULL)) AS labor_force_participation_pct,
        MAX(IF(indicator_code = 'SP.POP.TOTL',    value, NULL)) AS population
    FROM wb_latest
    GROUP BY iso_alpha_3
)

SELECT
    t.release_id,
    t.iso_alpha_2,
    m.iso_alpha_3,
    COALESCE(m.iso_country_name, t.iso_alpha_2) AS country_name,
    CAST(t.usage_count AS INTEGER) AS usage_count,
    t.usage_pct_of_global,
    d.distinct_tasks_observed,
    tt.top_task_text,
    tt.top_task_specialization_index,
    tt.top_task_country_pct,
    w.gdp_per_capita,
    w.labor_force_participation_pct,
    w.population,
    SAFE_DIVIDE(t.usage_count, SAFE_DIVIDE(w.population, 1000000)) AS usage_per_million_people
FROM country_totals t
LEFT JOIN iso_map              m ON m.iso_alpha_2 = t.iso_alpha_2
LEFT JOIN wb_pivot             w ON w.iso_alpha_3 = m.iso_alpha_3
LEFT JOIN top_task             tt ON tt.release_id = t.release_id AND tt.iso_alpha_2 = t.iso_alpha_2
LEFT JOIN distinct_task_count  d  ON d.release_id  = t.release_id AND d.iso_alpha_2  = t.iso_alpha_2
WHERE t.usage_count >= 200
ORDER BY t.release_id, t.usage_count DESC
