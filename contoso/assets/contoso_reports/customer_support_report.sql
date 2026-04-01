/* @bruin

name: contoso_reports.customer_support_report
type: bq.sql
description: |
  Monthly customer support performance report for Contoso's consumer electronics business.

  Aggregates support ticket data by communication channel (Phone, Email, Chat, Social) and
  issue category (Shipping, Returns, Product Quality, Billing, Account, Technical) to provide
  operational insights into support team efficiency, customer satisfaction trends, and
  resource utilization patterns.

  Key metrics include resolution rates, escalation patterns, average resolution times,
  customer satisfaction scores (1-5 scale), and agent workload distribution. This mart
  is used by support managers for capacity planning, performance tracking, and identifying
  improvement opportunities across different channels and issue types.

  Data is deduplicated by support_ticket_key using the most recent extraction timestamp
  to handle potential reprocessing scenarios.
connection: bruin-playground-eu
tags:
  - customer_support
  - operational_metrics
  - fact_table
  - monthly_aggregation
  - agent_utilization
  - satisfaction_tracking
  - resolution_metrics
  - channel_analysis

materialization:
  type: table
  strategy: create+replace

depends:
  - contoso_raw.support_tickets
  - contoso_staging.employee_directory


columns:
  - name: year
    type: INTEGER
    description: Calendar year of ticket creation (2018-2024 range in dataset)
    primary_key: true
  - name: month
    type: INTEGER
    description: Calendar month of ticket creation (1-12)
    primary_key: true
  - name: channel
    type: VARCHAR
    description: |
      Customer communication channel used for support request.
      Values: Phone (25%), Email (35%), Chat (30%), Social (10%)
    primary_key: true
  - name: category
    type: VARCHAR
    description: |
      Issue classification category for the support request.
      Common categories: Shipping, Returns, Product Quality, Billing, Account, Technical
    primary_key: true
  - name: ticket_count
    type: INTEGER
    description: Total number of support tickets created in this month/channel/category combination
  - name: resolved_count
    type: INTEGER
    description: |
      Number of tickets marked as 'Resolved' status. Excludes Open and Escalated tickets.
      Used to calculate resolution_rate metric
  - name: escalated_count
    type: INTEGER
    description: |
      Number of tickets that required escalation beyond first-level support.
      Indicates complexity or resource constraints for this segment
  - name: avg_resolution_days
    type: DOUBLE
    description: |
      Average number of days between ticket creation and resolution (rounded to 2 decimals).
      Only calculated for resolved tickets. Typical range: 0.4-2.3 days
  - name: avg_satisfaction
    type: DOUBLE
    description: |
      Average customer satisfaction score on 1-5 scale (rounded to 2 decimals).
      Based on post-resolution surveys with ~70% response rate. Higher scores indicate better service
  - name: critical_count
    type: INTEGER
    description: |
      Number of tickets classified as 'Critical' priority (~5% of total volume).
      Highest urgency level requiring immediate attention
  - name: high_count
    type: INTEGER
    description: |
      Number of tickets classified as 'High' priority (~15% of total volume).
      Second-highest urgency level after Critical
  - name: unique_agents
    type: INTEGER
    description: |
      Count of distinct support agents who handled tickets in this segment.
      Used for workload distribution analysis and capacity planning
  - name: tickets_per_agent
    type: DOUBLE
    description: |
      Average ticket workload per agent (ticket_count / unique_agents).
      Indicates resource utilization and potential capacity constraints
  - name: resolution_rate
    type: DOUBLE
    description: |
      Percentage of tickets resolved (resolved_count / ticket_count * 100).
      Key performance indicator for support effectiveness, typically 50-100%

@bruin */

WITH tickets_deduped AS (
    SELECT *
    FROM contoso_raw.support_tickets
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
ORDER BY year, month, channel, category
