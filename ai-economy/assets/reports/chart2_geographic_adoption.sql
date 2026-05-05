-- Chart 2: AI adoption per million people vs GDP per capita.
-- Filters to populations >= 1M so micro-states (Nauru with 12K residents, etc.)
-- don't dominate the per-capita view. Returns the country signature task lifted
-- from staging for tooltips.
SELECT
    iso_alpha_2,
    iso_alpha_3,
    country_name,
    usage_count,
    usage_pct_of_global,
    distinct_tasks_observed,
    top_task_text,
    top_task_specialization_index,
    top_task_country_pct,
    gdp_per_capita,
    labor_force_participation_pct,
    population,
    usage_per_million_people,
    -- Bucket GDP per capita into World Bank income groups for color encoding.
    CASE
        WHEN gdp_per_capita IS NULL THEN NULL
        WHEN gdp_per_capita <  1136 THEN 'Low income'
        WHEN gdp_per_capita <  4466 THEN 'Lower-middle'
        WHEN gdp_per_capita < 13846 THEN 'Upper-middle'
        ELSE 'High income'
    END AS income_group
FROM `bruin-playground-arsalan.staging.aei_geographic_adoption`
WHERE release_id = 'release_2026_01_15'
  AND population IS NOT NULL
  AND population >= 1000000
  AND usage_per_million_people IS NOT NULL
  AND gdp_per_capita IS NOT NULL
ORDER BY usage_per_million_people DESC
