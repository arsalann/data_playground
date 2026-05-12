-- Chart 3: Enterprise (1P API) vs consumer (Claude.ai) usage gap by SOC major.
-- For each occupation group: API share minus consumer share, in percentage points.
-- Positive => API-skewed (developers, automation), negative => consumer-skewed
-- (one-shot help, education). Sorted by absolute gap so the most-divergent
-- groups read first.
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
        c.api_count
    FROM `bruin-playground-arsalan.staging.aei_consumer_vs_api` c
    INNER JOIN `bruin-playground-arsalan.staging.aei_task_exposure` e USING (task_text)
    WHERE e.onet_soc_code IS NOT NULL
      AND c.consumer_pct IS NOT NULL
      AND c.api_pct IS NOT NULL
),

agg AS (
    SELECT
        n.soc_major,
        n.soc_name AS occupation_group,
        COUNT(*) AS shared_tasks,
        ROUND(SUM(j.consumer_pct), 3) AS consumer_pct,
        ROUND(SUM(j.api_pct), 3)      AS api_pct,
        ROUND(SUM(j.api_pct) - SUM(j.consumer_pct), 3) AS api_minus_consumer_pp
    FROM joined j
    INNER JOIN soc_names n ON n.soc_major = j.soc_major
    GROUP BY n.soc_major, n.soc_name
    HAVING shared_tasks >= 5
)

SELECT
    occupation_group,
    api_minus_consumer_pp,
    consumer_pct,
    api_pct,
    shared_tasks
FROM agg
ORDER BY api_minus_consumer_pp DESC
