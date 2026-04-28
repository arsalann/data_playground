{{ config(materialized='table') }}

WITH tickets_deduped AS (
    SELECT *
    FROM {{ source('contoso_raw', 'support_tickets') }}
    WHERE support_ticket_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY support_ticket_key
        ORDER BY extracted_at DESC
    ) = 1
),

monthly_stats AS (
    SELECT
        EXTRACT(YEAR FROM created_date) AS year,
        EXTRACT(MONTH FROM created_date) AS month,
        COUNT(*) AS ticket_count,
        COUNTIF(status = 'Resolved') AS resolved_count,
        COUNTIF(status = 'Open') AS open_count,
        COUNTIF(status = 'Escalated') AS escalated_count,
        ROUND(AVG(
            CASE WHEN resolved_date IS NOT NULL
                THEN DATE_DIFF(CAST(resolved_date AS DATE), CAST(created_date AS DATE), DAY)
            END
        ), 2) AS avg_resolution_days,
        ROUND(AVG(satisfaction_score), 2) AS avg_satisfaction,
        COUNTIF(priority = 'Critical') AS critical_count,
        COUNTIF(channel = 'Phone') AS phone_count,
        COUNTIF(channel = 'Email') AS email_count,
        COUNTIF(channel = 'Chat') AS chat_count
    FROM tickets_deduped
    GROUP BY 1, 2
),

top_categories AS (
    SELECT
        year,
        month,
        category,
        cnt,
        ROW_NUMBER() OVER (
            PARTITION BY year, month
            ORDER BY cnt DESC
        ) AS rn
    FROM (
        SELECT
            EXTRACT(YEAR FROM created_date) AS year,
            EXTRACT(MONTH FROM created_date) AS month,
            category,
            COUNT(*) AS cnt
        FROM tickets_deduped
        GROUP BY 1, 2, 3
    )
)

SELECT
    m.year,
    m.month,
    m.ticket_count,
    m.resolved_count,
    m.open_count,
    m.escalated_count,
    m.avg_resolution_days,
    m.avg_satisfaction,
    tc.category AS top_category,
    m.critical_count,
    m.phone_count,
    m.email_count,
    m.chat_count
FROM monthly_stats m
LEFT JOIN top_categories tc
    ON m.year = tc.year AND m.month = tc.month AND tc.rn = 1
