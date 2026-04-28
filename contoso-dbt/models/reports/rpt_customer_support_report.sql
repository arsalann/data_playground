{{ config(materialized='table') }}

WITH tickets_deduped AS (
    SELECT *
    FROM {{ source('contoso_raw', 'support_tickets') }}
    WHERE support_ticket_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY support_ticket_key
        ORDER BY extracted_at DESC
    ) = 1
)

SELECT
    EXTRACT(YEAR FROM created_date) AS year,
    EXTRACT(MONTH FROM created_date) AS month,
    channel,
    category,
    COUNT(*) AS ticket_count,
    COUNTIF(status = 'Resolved') AS resolved_count,
    COUNTIF(status = 'Escalated') AS escalated_count,
    ROUND(AVG(
        CASE WHEN resolved_date IS NOT NULL
            THEN DATE_DIFF(CAST(resolved_date AS DATE), CAST(created_date AS DATE), DAY)
        END
    ), 2) AS avg_resolution_days,
    ROUND(AVG(satisfaction_score), 2) AS avg_satisfaction,
    COUNTIF(priority = 'Critical') AS critical_count,
    COUNTIF(priority = 'High') AS high_count,
    COUNT(DISTINCT agent_employee_key) AS unique_agents,
    ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT agent_employee_key), 0), 2) AS tickets_per_agent,
    ROUND(COUNTIF(status = 'Resolved') / COUNT(*) * 100, 2) AS resolution_rate
FROM tickets_deduped
GROUP BY 1, 2, 3, 4
