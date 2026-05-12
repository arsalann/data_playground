-- Chart 2: Per-capita Claude adoption (per million people) vs GDP per capita.
-- Restricts to populations >= 1M so micro-states don't dominate the per-capita
-- view. Raw values; the chart uses Recharts log-scale on both axes (xScale /
-- yScale = "log") so the rendered ticks are real currency / per-million
-- numbers rather than log-transformed indices.
SELECT
    country_name,
    iso_alpha_3,
    ROUND(gdp_per_capita, 0)            AS gdp_per_capita,
    ROUND(usage_per_million_people, 2)  AS usage_per_million_people
FROM `bruin-playground-arsalan.staging.aei_geographic_adoption`
WHERE release_id = 'release_2026_01_15'
  AND population IS NOT NULL
  AND population >= 1000000
  AND usage_per_million_people IS NOT NULL
  AND usage_per_million_people > 0
  AND gdp_per_capita IS NOT NULL
  AND gdp_per_capita > 0
ORDER BY usage_per_million_people DESC
