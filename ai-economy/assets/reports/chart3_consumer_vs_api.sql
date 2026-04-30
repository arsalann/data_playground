-- Chart 3: Consumer (Claude.ai) vs 1P API usage by occupation group.
-- Aggregates the per-task share gap to the SOC major level so the chart can
-- show "Education is 4x more consumer-skewed than the API average" instead of
-- 1,800 task-description rows.
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

joined AS (
    SELECT
        LEFT(e.onet_soc_code, 2) AS soc_major,
        c.consumer_pct,
        c.api_pct,
        c.consumer_count,
        c.api_count,
        c.api_cost_index,
        c.api_prompt_tokens_index,
        c.api_completion_tokens_index
    FROM `bruin-playground-arsalan.staging.aei_consumer_vs_api` c
    INNER JOIN `bruin-playground-arsalan.staging.aei_task_exposure` e USING (task_text)
    WHERE e.onet_soc_code IS NOT NULL
      AND c.consumer_pct IS NOT NULL
      AND c.api_pct IS NOT NULL
)

SELECT
    n.soc_major,
    n.soc_name AS occupation_group,
    COUNT(*)                                                    AS shared_tasks,
    SUM(j.consumer_pct)                                         AS consumer_pct,
    SUM(j.api_pct)                                              AS api_pct,
    SUM(j.api_pct) - SUM(j.consumer_pct)                        AS delta_pp,
    SUM(j.consumer_count)                                       AS consumer_count,
    SUM(j.api_count)                                            AS api_count,
    -- Cost / token indices weighted by API usage count so heavy tasks dominate.
    SAFE_DIVIDE(SUM(j.api_cost_index             * j.api_count), SUM(j.api_count)) AS api_cost_index,
    SAFE_DIVIDE(SUM(j.api_prompt_tokens_index    * j.api_count), SUM(j.api_count)) AS api_prompt_tokens_index,
    SAFE_DIVIDE(SUM(j.api_completion_tokens_index* j.api_count), SUM(j.api_count)) AS api_completion_tokens_index
FROM joined j
INNER JOIN soc_names n ON n.soc_major = j.soc_major
GROUP BY n.soc_major, n.soc_name
HAVING shared_tasks >= 5
ORDER BY ABS(delta_pp) DESC
