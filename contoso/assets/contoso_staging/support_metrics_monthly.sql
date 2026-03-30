/* @bruin

name: contoso_staging.support_metrics_monthly
type: bq.sql
description: |
  Comprehensive monthly customer support performance dashboard for Contoso Electronics,
  aggregating 150K support tickets across multiple channels, categories, and priority levels.

  This staging table transforms raw support ticket data into business-ready KPIs for operational
  reporting and performance analysis. Provides complete monthly rollups of ticket volume,
  resolution metrics, customer satisfaction, and channel/priority breakdowns from 2016-2026.

  Key business metrics include:
  - Ticket volume trends: ~1,136 tickets/month average across all channels
  - Resolution performance: 85% resolution rate, ~1.5 day average resolution time
  - Channel distribution: Email (35%), Chat (30%), Phone (25%), Social (10%)
  - Customer satisfaction: 3.7/5.0 average score with ~70% survey response rate
  - Priority escalation: 5% critical tickets, 5% escalated to specialists

  Data transformations applied:
  - Deduplicates raw tickets using extracted_at timestamp (most recent wins)
  - Aggregates by created_date month for consistent time-series reporting
  - Calculates resolution time only for completed tickets (excludes open/escalated)
  - Determines most common category per month using row_number ranking
  - Computes satisfaction averages only from survey responses (excludes nulls)
  - Groups tickets by communication channel (Phone, Email, Chat) - Social excluded from channel counts

  Typical use cases:
  - Monthly operational reporting and SLA tracking
  - Customer service capacity planning and staffing decisions
  - Channel effectiveness analysis and resource allocation
  - Customer satisfaction monitoring and process improvement
  - Executive dashboards showing support organization health

  Operational characteristics:
  - Refreshed daily via create+replace strategy, but data granularity is monthly
  - Contains exactly 132 rows (11 years × 12 months) covering full business history
  - Most common categories are "Product Quality" and "Shipping" issues
  - Resolution times are highly consistent (1.4-1.6 days) indicating mature processes
  - No missing data due to synthetic generation ensuring comprehensive coverage
connection: gcp-default
tags:
  - customer_support
  - operational_metrics
  - staging
  - fact_table
  - monthly_grain
  - satisfaction_tracking
  - channel_analysis
  - sla_monitoring
  - consumer_electronics
  - kpi_dashboard

materialization:
  type: table
  strategy: create+replace

depends:
  - contoso_raw.support_tickets
  - contoso_raw.customers


columns:
  - name: year
    type: INTEGER
    description: Calendar year for the reporting period (2016-2026). Used with month to form composite primary key for time-series analysis and trend reporting.
    primary_key: true
    checks:
      - name: not_null
  - name: month
    type: INTEGER
    description: Calendar month number (1-12). Combined with year forms unique time period identifier. Enables seasonal analysis and month-over-month comparisons.
    primary_key: true
    checks:
      - name: not_null
  - name: ticket_count
    type: INTEGER
    description: Total support tickets created during the month across all channels and categories. Averages ~1,136 tickets/month. Primary volume metric for capacity planning and staffing decisions.
    checks:
      - name: not_null
      - name: positive
  - name: resolved_count
    type: INTEGER
    description: Number of tickets successfully resolved during the month. Averages ~965 tickets (85% resolution rate). Key SLA metric for customer service effectiveness.
    checks:
      - name: not_null
      - name: positive
  - name: open_count
    type: INTEGER
    description: Tickets remaining open at month end requiring follow-up or additional customer interaction. Averages ~115 tickets (10% of volume). Important for workload planning.
    checks:
      - name: not_null
      - name: positive
  - name: escalated_count
    type: INTEGER
    description: Tickets escalated to specialized teams or senior agents during the month. Averages ~56 tickets (5% escalation rate). Indicator of complex issue volume.
    checks:
      - name: not_null
      - name: positive
  - name: avg_resolution_days
    type: DOUBLE
    description: Average time in days from ticket creation to resolution for resolved tickets only. Typically 1.4-1.6 days. Critical SLA metric for customer experience measurement.
    checks:
      - name: not_null
      - name: positive
  - name: avg_satisfaction
    type: DOUBLE
    description: Average customer satisfaction score on 1-5 scale (5=Very Satisfied) from post-resolution surveys. Based on ~70% response rate. Ranges 3.56-3.80, averaging 3.7/5.0.
    checks:
      - name: not_null
      - name: positive
  - name: top_category
    type: VARCHAR
    description: Most frequently reported issue category for the month. Typically alternates between "Product Quality" and "Shipping" based on seasonal patterns and business operations.
    checks:
      - name: not_null
  - name: critical_count
    type: INTEGER
    description: Number of tickets assigned Critical priority level during the month. Averages ~57 tickets (5% of volume). High-impact issues requiring immediate attention.
    checks:
      - name: not_null
      - name: positive
  - name: phone_count
    type: INTEGER
    description: Tickets submitted via phone channel during the month. Averages ~285 tickets (25% of volume). Typically urgent or complex issues requiring voice interaction.
    checks:
      - name: not_null
      - name: positive
  - name: email_count
    type: INTEGER
    description: Tickets submitted via email channel during the month. Averages ~398 tickets (35% of volume). Most common channel for detailed inquiries and documentation.
    checks:
      - name: not_null
      - name: positive
  - name: chat_count
    type: INTEGER
    description: Tickets submitted via chat channel during the month. Averages ~340 tickets (30% of volume). Preferred channel for quick questions and real-time support.
    checks:
      - name: not_null
      - name: positive

@bruin */

WITH tickets_deduped AS (
    SELECT *
    FROM contoso_raw.support_tickets
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
ORDER BY m.year, m.month
