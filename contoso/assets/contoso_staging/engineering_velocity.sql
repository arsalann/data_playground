/* @bruin

name: contoso_staging.engineering_velocity
type: bq.sql
description: |
  Engineering velocity and deployment quality metrics aggregated by sprint for Contoso's engineering organization.

  This asset combines JIRA-like sprint ticket data with production deployment data to provide comprehensive insights
  into engineering team performance, delivery velocity, and deployment success rates. Sprints are matched to
  deployments using ISO week alignment, providing a standardized view of development cycles.

  The data spans from 2016 to 2026 covering ~571 unique sprints with metrics on story point completion, cycle times,
  bug-to-feature ratios, and production deployment health. Sprint names follow ISO week format (e.g., "Sprint 2024-W03").

  Key business uses:
  - Engineering productivity tracking and goal setting
  - Sprint planning and capacity forecasting
  - Deployment frequency and quality monitoring
  - Technical debt analysis via bug-to-story ratios
  - Release management and rollback trend analysis

  Data peculiarities:
  - Rollback rate is null (~39% of sprints) when no deployments occurred during sprint week
  - Some sprints may have zero deployments due to sprint/deployment timing misalignment
  - Cycle time only calculated for resolved tickets (Done/Closed status)
  - Tasks and Improvements are excluded from story point calculations
connection: bruin-playground-eu
tags:
  - domain:engineering
  - domain:product
  - data_type:fact_table
  - update_pattern:snapshot
  - pipeline_role:mart
  - sensitivity:internal
  - grain:sprint_level

materialization:
  type: table
  strategy: create+replace

depends:
  - contoso_raw.sprint_tickets
  - contoso_raw.deployments
  - contoso_raw.employees


columns:
  - name: sprint_name
    type: VARCHAR
    description: |
      Sprint identifier following ISO week format (e.g., "Sprint 2024-W03").
      Unique identifier for each sprint cycle based on ISO week of ticket creation dates.
    primary_key: true
  - name: sprint_start_date
    type: DATE
    description: |
      Earliest ticket creation date within the sprint. Represents the effective start
      of development work for sprint planning and velocity calculations.
  - name: total_tickets
    type: INTEGER
    description: |
      Total count of all ticket types (Bug, Story, Task, Improvement) included in the sprint.
      Used for sprint capacity and workload analysis.
  - name: completed_tickets
    type: INTEGER
    description: |
      Count of tickets with 'Done' or 'Closed' status. Primary metric for sprint
      delivery success rate and velocity measurement.
  - name: bug_count
    type: INTEGER
    description: |
      Count of tickets with type 'Bug'. Used for technical debt tracking and
      quality analysis relative to feature development.
  - name: story_count
    type: INTEGER
    description: |
      Count of tickets with type 'Story'. Represents new feature development
      work and primary business value delivery.
  - name: total_story_points
    type: INTEGER
    description: |
      Sum of story points estimated for all tickets in sprint (Fibonacci scale 1,2,3,5,8,13).
      Used for sprint capacity planning and effort estimation.
  - name: completed_story_points
    type: INTEGER
    description: |
      Sum of story points for tickets marked 'Done' or 'Closed'. Key velocity
      metric for tracking team delivery against planned capacity.
  - name: avg_cycle_time_days
    type: DOUBLE
    description: |
      Average days from ticket creation to resolution for resolved tickets only.
      Key metric for process efficiency and delivery predictability. Null when no tickets resolved.
  - name: deployment_count
    type: INTEGER
    description: |
      Count of production deployments during the sprint's ISO week. Measures deployment
      frequency and continuous delivery maturity. Zero when no deployments occurred.
  - name: rollback_count
    type: INTEGER
    description: |
      Count of production deployments with 'Rolled Back' status during sprint week.
      Critical metric for deployment quality and stability monitoring.
  - name: rollback_rate
    type: DOUBLE
    description: |-
      Percentage of deployments that required rollback (rollback_count/deployment_count * 100).
      Key metric for deployment success rate and release quality. Null when deployment_count is zero.

@bruin */

WITH tickets_deduped AS (
    SELECT *
    FROM contoso_raw.sprint_tickets
    WHERE ticket_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ticket_key
        ORDER BY extracted_at DESC
    ) = 1
),

deployments_deduped AS (
    SELECT *
    FROM contoso_raw.deployments
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

-- Match deployments to sprints by week
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
ORDER BY sm.sprint_start_date
