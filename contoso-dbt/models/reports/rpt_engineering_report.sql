{{ config(materialized='table') }}

SELECT
    EXTRACT(YEAR FROM sprint_start_date) AS year,
    EXTRACT(MONTH FROM sprint_start_date) AS month,
    COUNT(*) AS total_sprints,
    SUM(total_tickets) AS total_tickets,
    SUM(completed_tickets) AS completed_tickets,
    ROUND(SAFE_DIVIDE(SUM(completed_tickets), NULLIF(SUM(total_tickets), 0)) * 100, 2) AS completion_rate,
    SUM(total_story_points) AS total_story_points,
    SUM(completed_story_points) AS completed_story_points,
    ROUND(SAFE_DIVIDE(SUM(completed_story_points), NULLIF(COUNT(*), 0)), 2) AS avg_velocity,
    SUM(bug_count) AS total_bugs,
    ROUND(SAFE_DIVIDE(SUM(bug_count), NULLIF(SUM(total_tickets), 0)) * 100, 2) AS bug_ratio,
    ROUND(AVG(avg_cycle_time_days), 2) AS avg_cycle_time_days,
    SUM(deployment_count) AS total_deployments,
    SUM(rollback_count) AS total_rollbacks,
    ROUND(AVG(rollback_rate), 2) AS avg_rollback_rate,
    ROUND(SAFE_DIVIDE(SUM(deployment_count), NULLIF(COUNT(*), 0)), 2) AS deploy_frequency
FROM {{ ref('stg_engineering_velocity') }}
GROUP BY 1, 2
