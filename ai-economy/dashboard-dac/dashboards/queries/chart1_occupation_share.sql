-- Chart 1: Pareto of Claude conversation share by BLS SOC major group.
-- Aggregates the per-O*NET-task `aei_task_exposure` rows up to the 22
-- BLS major groups, computes each group's share of global Claude usage,
-- and the cumulative share so the combo chart can overlay a cum-share line
-- on top of the per-group bars.
WITH soc_names AS (
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
),

base AS (
    SELECT
        LEFT(onet_soc_code, 2) AS soc_major,
        ai_autonomy_mean,
        bls_median_annual_wage,
        usage_count_global
    FROM `bruin-playground-arsalan.staging.aei_task_exposure`
    WHERE onet_soc_code IS NOT NULL
      AND ai_autonomy_mean IS NOT NULL
      AND usage_count_global IS NOT NULL
),

agg AS (
    SELECT
        n.soc_major,
        n.soc_name AS occupation_group,
        SUM(b.usage_count_global) AS usage_count_total,
        SAFE_DIVIDE(SUM(b.ai_autonomy_mean * b.usage_count_global), SUM(b.usage_count_global)) AS ai_autonomy_mean,
        SAFE_DIVIDE(SUM(b.bls_median_annual_wage * b.usage_count_global), SUM(b.usage_count_global)) AS median_wage
    FROM base b
    INNER JOIN soc_names n ON n.soc_major = b.soc_major
    GROUP BY n.soc_major, n.soc_name
    HAVING usage_count_total >= 200
),

sized AS (
    SELECT
        occupation_group,
        usage_count_total,
        ai_autonomy_mean,
        median_wage,
        SAFE_DIVIDE(usage_count_total, SUM(usage_count_total) OVER ()) * 100 AS share_pct
    FROM agg
),

ordered AS (
    SELECT
        occupation_group,
        ROUND(share_pct, 2) AS share_pct,
        ROUND(SUM(share_pct) OVER (
            ORDER BY share_pct DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 1) AS cum_share_pct,
        ROUND(ai_autonomy_mean, 2) AS ai_autonomy_mean,
        ROUND(median_wage, 0) AS median_wage_usage_weighted,
        usage_count_total
    FROM sized
)

SELECT *
FROM ordered
ORDER BY share_pct DESC
