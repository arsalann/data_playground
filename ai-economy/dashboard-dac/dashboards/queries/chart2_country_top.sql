-- Top-15 countries by Claude conversations per million people (population >= 1M).
-- `usage_pct_of_global` from staging is already a percentage (e.g. United States
-- = 21.97 means 21.97 % of all classified Claude conversations come from the
-- US), so it is shown as-is - no extra * 100 scaling.
SELECT
    country_name AS country,
    ROUND(usage_per_million_people, 1) AS usage_per_million,
    ROUND(usage_pct_of_global, 2)      AS share_of_global_pct,
    ROUND(gdp_per_capita, 0)           AS gdp_per_capita_usd,
    population
FROM `bruin-playground-arsalan.staging.aei_geographic_adoption`
WHERE release_id = 'release_2026_01_15'
  AND population IS NOT NULL
  AND population >= 1000000
  AND usage_per_million_people IS NOT NULL
  AND gdp_per_capita IS NOT NULL
ORDER BY usage_per_million_people DESC
LIMIT 15
