SELECT
    iso3_code,
    country_name,
    region,
    pension_spending_pct_gdp_latest,
    old_age_dep_ratio_today,
    old_age_dep_ratio_2050,
    pension_assets_pct_gdp_latest
FROM `bruin-playground-arsalan.staging.pc_country_pension_profile`
WHERE pension_spending_pct_gdp_latest IS NOT NULL
   OR old_age_dep_ratio_today IS NOT NULL
ORDER BY country_name
