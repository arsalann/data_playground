-- Chart 4: Collaboration mix by occupation group (SOC major).
-- Pivots the 5 collaboration types into one row per SOC major weighted by
-- AEI conversation count. Excludes 'none' / 'not_classified'.
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

agg AS (
    SELECT
        soc_major,
        SUM(usage_count_global) AS usage_count_total,
        SAFE_DIVIDE(SUM(collab_directive_pct       * usage_count_global), SUM(usage_count_global)) AS directive_pct,
        SAFE_DIVIDE(SUM(collab_task_iteration_pct  * usage_count_global), SUM(usage_count_global)) AS iteration_pct,
        SAFE_DIVIDE(SUM(collab_feedback_loop_pct   * usage_count_global), SUM(usage_count_global)) AS feedback_pct,
        SAFE_DIVIDE(SUM(collab_validation_pct      * usage_count_global), SUM(usage_count_global)) AS validation_pct,
        SAFE_DIVIDE(SUM(collab_learning_pct        * usage_count_global), SUM(usage_count_global)) AS learning_pct
    FROM base
    GROUP BY soc_major
    HAVING usage_count_total >= 1000
)

-- Unpivot to long format so Altair can render a stacked bar by collaboration type.
SELECT
    n.soc_major,
    n.soc_name AS occupation_group,
    a.usage_count_total,
    collab.collaboration_type,
    collab.share_pct
FROM agg a
INNER JOIN soc_names n ON n.soc_major = a.soc_major
CROSS JOIN UNNEST([
    STRUCT('Directive (delegate)' AS collaboration_type, a.directive_pct AS share_pct),
    STRUCT('Task iteration',                              a.iteration_pct),
    STRUCT('Feedback loop (debug)',                       a.feedback_pct),
    STRUCT('Validation',                                  a.validation_pct),
    STRUCT('Learning',                                    a.learning_pct)
]) AS collab
WHERE collab.share_pct IS NOT NULL
ORDER BY a.usage_count_total DESC, collab.collaboration_type
