/* @bruin

name: contoso_reports.engineering_report
type: bq.sql
description: |
  Monthly engineering team report for Contoso's consumer electronics retail engineering organization.

  This executive dashboard aggregates sprint-level engineering velocity and DORA metrics into monthly rollups
  for leadership reporting and trend analysis. Combines agile delivery metrics (velocity, story points,
  completion rates) with DevOps maturity indicators (deployment frequency, rollback rates, cycle times).

  Data is sourced from JIRA-like sprint tracking and production deployment logs, providing comprehensive
  insights into both development productivity and operational reliability. Used for engineering capacity
  planning, performance reviews, and identifying process improvement opportunities.

  Business context:
  - Tracks engineering performance across multiple product teams at Contoso
  - Supports monthly leadership reviews and quarterly business planning
  - Monitors progress against engineering OKRs and DORA benchmark goals
  - Identifies trends in technical debt (via bug ratios) and delivery predictability

  Data characteristics:
  - Monthly grain from 2016-2026, covering ~131 months of engineering activity
  - Aggregates from ~571 sprint-level records in contoso_staging.engineering_velocity
  - Average ~4.4 sprints per month with ~61 tickets per month across all teams
  - Deploy frequency averages ~0.9 deployments per sprint (lower than industry benchmarks)
  - Bug ratio averages ~25% of total tickets, indicating moderate technical debt levels
  - Some months may have null rollback rates when no deployments occurred
connection: bruin-playground-eu
tags:
  - domain:engineering
  - domain:product
  - data_type:fact_table
  - update_pattern:snapshot
  - pipeline_role:mart
  - sensitivity:internal
  - grain:monthly
  - executive_reporting

materialization:
  type: table
  strategy: create+replace

depends:
  - contoso_staging.engineering_velocity

secrets:
  - key: bruin-playground-eu
    inject_as: bruin-playground-eu

columns:
  - name: year
    type: INTEGER
    description: |
      Calendar year for the reporting period (2016-2026).
      Used for year-over-year trend analysis and annual planning cycles.
    primary_key: true
  - name: month
    type: INTEGER
    description: |
      Calendar month (1-12) for the reporting period.
      Combined with year forms the monthly reporting grain for executive dashboards.
    primary_key: true
  - name: total_sprints
    type: INTEGER
    description: |
      Number of distinct sprints that occurred within the calendar month.
      Indicates engineering team capacity and sprint planning frequency. Typically 4-5 sprints per month.
  - name: total_tickets
    type: INTEGER
    description: |
      Total count of all JIRA-style tickets (Story, Bug, Task, Improvement) across all sprints in the month.
      Primary indicator of engineering workload and capacity utilization. Averages ~61 tickets per month.
  - name: completed_tickets
    type: INTEGER
    description: |
      Count of tickets marked 'Done' or 'Closed' status across all sprints in the month.
      Key delivery metric for measuring actual throughput against planned capacity. Typically ~46 tickets per month.
  - name: completion_rate
    type: DOUBLE
    description: |
      Percentage of planned tickets actually completed in the month (completed_tickets/total_tickets * 100).
      Critical KPI for sprint planning accuracy and team predictability. Ranges 60-88%, averages ~74%.
      Values below 70% may indicate capacity overcommitment or scope creep.
  - name: total_story_points
    type: INTEGER
    description: |
      Sum of Fibonacci-scale story points (1,2,3,5,8,13) estimated for all tickets in the month.
      Used for capacity planning and workload normalization across different ticket types. Averages ~236 points monthly.
  - name: completed_story_points
    type: INTEGER
    description: |
      Sum of story points for tickets completed in the month.
      Primary velocity metric for measuring actual delivery against estimated effort. Averages ~177 points monthly.
  - name: avg_velocity
    type: DOUBLE
    description: |
      Average story points delivered per sprint within the month (completed_story_points/total_sprints).
      Core agile metric for sprint planning and capacity forecasting. Ranges 20-61 points per sprint, averages ~41.
      Consistent velocity indicates mature estimation practices.
  - name: total_bugs
    type: INTEGER
    description: |
      Count of tickets with type 'Bug' across all sprints in the month.
      Technical debt indicator and quality metric. Averages ~15 bugs per month.
      Trending upward may indicate code quality issues or insufficient testing.
  - name: bug_ratio
    type: DOUBLE
    description: |
      Percentage of total tickets that are bugs (total_bugs/total_tickets * 100).
      Key quality metric for technical debt monitoring. Ranges 12-40%, averages ~25%.
      Values above 30% typically indicate quality issues requiring attention.
  - name: avg_cycle_time_days
    type: DOUBLE
    description: |
      Average days from ticket creation to resolution for completed tickets only.
      DORA-adjacent metric indicating process efficiency and delivery predictability.
      Ranges 10-18 days, averages ~15 days. Excludes unresolved tickets to avoid skew.
  - name: total_deployments
    type: INTEGER
    description: |
      Count of production deployments across all sprints in the month.
      DORA deployment frequency indicator. Averages ~4 deployments per month.
      Lower values may indicate deployment pipeline bottlenecks or release process friction.
  - name: total_rollbacks
    type: INTEGER
    description: |
      Count of production deployments that required rollback due to issues.
      Critical reliability and change failure rate metric. Typically 0-1 per month, averages ~0.3.
      Multiple rollbacks in a month indicate quality gate or testing process gaps.
  - name: avg_rollback_rate
    type: DOUBLE
    description: |
      Average percentage of deployments requiring rollback (total_rollbacks/total_deployments * 100).
      DORA change failure rate metric for deployment quality assessment. Ranges 0-100%, averages ~6%.
      Null when no deployments occurred in the month (~5% of records). Industry benchmark is <15%.
  - name: deploy_frequency
    type: DOUBLE
    description: |-
      Average deployments per sprint within the month (total_deployments/total_sprints).
      DORA deployment frequency metric indicating CI/CD maturity. Ranges 0-2, averages ~0.9 per sprint.
      Values below 1.0 suggest opportunity for more frequent, smaller releases.

@bruin */

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
FROM contoso_staging.engineering_velocity
GROUP BY 1, 2
ORDER BY year, month
