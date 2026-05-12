-- Chart 1: AI usage concentration by occupation group (SOC major).
-- Aggregates from individual O*NET tasks to the 22 BLS SOC major groups so the
-- chart shows interpretable category names (e.g. "Computer & Math") instead of
-- 2,500+ task description sentences.
WITH base AS (
    SELECT
        LEFT(onet_soc_code, 2) AS soc_major,
        bls_total_employment,
        bls_median_annual_wage,
        usage_count_global,
        ai_autonomy_mean,
        human_only_time_mean,
        human_with_ai_time_mean,
        soc_code_6digit
    FROM `bruin-playground-arsalan.staging.aei_task_exposure`
    WHERE onet_soc_code IS NOT NULL
      AND ai_autonomy_mean IS NOT NULL
      AND usage_count_global IS NOT NULL
),

-- BLS major-group SOC names. Hard-coded because the 22 root codes are stable
-- and BLS does not ship a major-group dimension table inline with OEWS.
soc_names AS (
    SELECT * FROM UNNEST([
        STRUCT('11' AS soc_major, 'Management' AS soc_name),
        STRUCT('13', 'Business & Finance'),
        STRUCT('15', 'Computer & Math'),
        STRUCT('17', 'Architecture & Engineering'),
        STRUCT('19', 'Life, Physical & Social Sciences'),
        STRUCT('21', 'Community & Social Services'),
        STRUCT('23', 'Legal'),
        STRUCT('25', 'Education & Library'),
        STRUCT('27', 'Arts, Design & Media'),
        STRUCT('29', 'Healthcare Practitioners'),
        STRUCT('31', 'Healthcare Support'),
        STRUCT('33', 'Protective Services'),
        STRUCT('35', 'Food Preparation & Serving'),
        STRUCT('37', 'Building & Grounds Cleaning'),
        STRUCT('39', 'Personal Care & Service'),
        STRUCT('41', 'Sales'),
        STRUCT('43', 'Office & Admin Support'),
        STRUCT('45', 'Farming, Fishing & Forestry'),
        STRUCT('47', 'Construction & Extraction'),
        STRUCT('49', 'Installation & Repair'),
        STRUCT('51', 'Production'),
        STRUCT('53', 'Transportation & Material Moving')
    ])
)

SELECT
    n.soc_major,
    n.soc_name AS occupation_group,
    SUM(b.usage_count_global)                                                                      AS usage_count_total,
    COUNT(DISTINCT b.soc_code_6digit)                                                              AS occupations_observed,
    SAFE_DIVIDE(SUM(b.ai_autonomy_mean   * b.usage_count_global), SUM(b.usage_count_global))       AS ai_autonomy_mean,
    SAFE_DIVIDE(SUM(b.human_only_time_mean * b.usage_count_global), SUM(b.usage_count_global))     AS human_only_time_mean,
    SAFE_DIVIDE(SUM(b.human_with_ai_time_mean * b.usage_count_global), SUM(b.usage_count_global))  AS human_with_ai_time_mean,
    -- Wage and employment weighted by AEI conversation count so the value reflects
    -- the wage workers actually using Claude bring, not a population-level mean.
    SAFE_DIVIDE(SUM(b.bls_median_annual_wage * b.usage_count_global), SUM(b.usage_count_global))   AS median_wage_usage_weighted,
    SUM(b.bls_total_employment)                                                                    AS total_us_employment,
    CASE
        WHEN SAFE_DIVIDE(SUM(b.ai_autonomy_mean*b.usage_count_global), SUM(b.usage_count_global)) IS NULL THEN NULL
        WHEN SAFE_DIVIDE(SUM(b.ai_autonomy_mean*b.usage_count_global), SUM(b.usage_count_global)) < 2.5 THEN 'Augmentation'
        WHEN SAFE_DIVIDE(SUM(b.ai_autonomy_mean*b.usage_count_global), SUM(b.usage_count_global)) < 3.5 THEN 'Hybrid'
        ELSE 'Automation'
    END AS exposure_pattern
FROM base b
INNER JOIN soc_names n ON n.soc_major = b.soc_major
GROUP BY n.soc_major, n.soc_name
HAVING usage_count_total >= 200
ORDER BY usage_count_total DESC
