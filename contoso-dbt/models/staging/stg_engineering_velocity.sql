{{ config(materialized='table') }}

WITH tickets_deduped AS (
    SELECT *
    FROM {{ source('contoso_raw', 'sprint_tickets') }}
    WHERE ticket_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ticket_key
        ORDER BY extracted_at DESC
    ) = 1
),

deployments_deduped AS (
    SELECT *
    FROM {{ source('contoso_raw', 'deployments') }}
    WHERE deployment_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY deployment_key
        ORDER BY extracted_at DESC
    ) = 1
),

sprint_metrics AS (
    SELECT
        sprint_name,
        MIN(CAST(created_date AS DATE)) AS sprint_start_date,
        COUNT(*) AS total_tickets,
        COUNTIF(status IN ('Done', 'Closed')) AS completed_tickets,
        COUNTIF(ticket_type = 'Bug') AS bug_count,
        COUNTIF(ticket_type = 'Story') AS story_count,
        SUM(story_points) AS total_story_points,
        SUM(CASE WHEN status IN ('Done', 'Closed') THEN story_points ELSE 0 END) AS completed_story_points,
        ROUND(AVG(
            CASE WHEN resolved_date IS NOT NULL
                THEN DATE_DIFF(CAST(resolved_date AS DATE), CAST(created_date AS DATE), DAY)
            END
        ), 2) AS avg_cycle_time_days
    FROM tickets_deduped
    GROUP BY sprint_name
),

deploy_by_week AS (
    SELECT
        CONCAT('Sprint ', EXTRACT(ISOYEAR FROM deploy_date), '-W',
            LPAD(CAST(EXTRACT(ISOWEEK FROM deploy_date) AS STRING), 2, '0')) AS sprint_name,
        COUNT(*) AS deployment_count,
        COUNTIF(status = 'Rolled Back') AS rollback_count
    FROM deployments_deduped
    WHERE environment = 'Production'
    GROUP BY 1
)

SELECT
    sm.sprint_name,
    sm.sprint_start_date,
    sm.total_tickets,
    sm.completed_tickets,
    sm.bug_count,
    sm.story_count,
    sm.total_story_points,
    sm.completed_story_points,
    sm.avg_cycle_time_days,
    COALESCE(dw.deployment_count, 0) AS deployment_count,
    COALESCE(dw.rollback_count, 0) AS rollback_count,
    ROUND(SAFE_DIVIDE(dw.rollback_count, NULLIF(dw.deployment_count, 0)) * 100, 2) AS rollback_rate
FROM sprint_metrics sm
LEFT JOIN deploy_by_week dw ON sm.sprint_name = dw.sprint_name
