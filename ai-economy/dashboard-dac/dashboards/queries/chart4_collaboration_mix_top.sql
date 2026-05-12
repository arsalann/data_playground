-- Chart 4 (top half): Top-10 BLS major groups by AEI conversation volume.
-- Splitting into two charts of 10 categories each (top + bottom by usage)
-- so the x-axis labels fit at full chart width without rotation overlap.
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
        LEFT(e.onet_soc_code, 2) AS soc_major,
        e.usage_count_global,
        e.collab_directive_pct,
        e.collab_task_iteration_pct,
        e.collab_feedback_loop_pct,
        e.collab_validation_pct,
        e.collab_learning_pct
    FROM `bruin-playground-arsalan.staging.aei_task_exposure` e
    WHERE e.onet_soc_code IS NOT NULL
      AND e.usage_count_global IS NOT NULL
),

weighted AS (
    SELECT
        soc_major,
        SUM(usage_count_global) AS usage_count_total,
        SAFE_DIVIDE(SUM(collab_directive_pct      * usage_count_global), SUM(usage_count_global)) AS d,
        SAFE_DIVIDE(SUM(collab_task_iteration_pct * usage_count_global), SUM(usage_count_global)) AS i,
        SAFE_DIVIDE(SUM(collab_feedback_loop_pct  * usage_count_global), SUM(usage_count_global)) AS f,
        SAFE_DIVIDE(SUM(collab_validation_pct     * usage_count_global), SUM(usage_count_global)) AS v,
        SAFE_DIVIDE(SUM(collab_learning_pct       * usage_count_global), SUM(usage_count_global)) AS l
    FROM base
    GROUP BY soc_major
    HAVING usage_count_total >= 1000
       AND (d + i + f + v + l) > 0
),

normalised AS (
    SELECT
        n.soc_name AS occupation_group,
        ROUND(SAFE_DIVIDE(w.d, w.d + w.i + w.f + w.v + w.l) * 100, 1) AS directive,
        ROUND(SAFE_DIVIDE(w.i, w.d + w.i + w.f + w.v + w.l) * 100, 1) AS task_iteration,
        ROUND(SAFE_DIVIDE(w.f, w.d + w.i + w.f + w.v + w.l) * 100, 1) AS feedback_loop,
        ROUND(SAFE_DIVIDE(w.v, w.d + w.i + w.f + w.v + w.l) * 100, 1) AS validation,
        ROUND(SAFE_DIVIDE(w.l, w.d + w.i + w.f + w.v + w.l) * 100, 1) AS learning,
        w.usage_count_total
    FROM weighted w
    INNER JOIN soc_names n ON n.soc_major = w.soc_major
)

SELECT *
FROM normalised
ORDER BY usage_count_total DESC
LIMIT 10
