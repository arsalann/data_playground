SELECT
    iso3_code,
    country_name,
    region,
    public_debt_pct_gdp_latest,
    public_debt_pct_gdp_year,
    public_debt_pct_gdp_2030,
    old_age_dep_ratio_today,
    old_age_dep_ratio_2050,
    pension_spending_pct_gdp_latest
FROM `bruin-playground-arsalan.staging.pc_country_pension_profile`
WHERE public_debt_pct_gdp_latest IS NOT NULL
  AND old_age_dep_ratio_2050 IS NOT NULL
ORDER BY country_name
