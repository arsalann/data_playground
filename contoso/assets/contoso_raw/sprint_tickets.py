"""@bruin

name: contoso_raw.sprint_tickets
description: |
  Engineering sprint tickets from Contoso's Agile development process, simulating a Jira-like ticketing system.

  This dataset contains ~8,000 synthetic tickets representing realistic engineering work patterns across
  different ticket types (Bug fixes, User Stories, Tasks, Improvements) with proper Agile methodology
  attributes like story points, sprints, and status workflows.

  The data spans 10+ years (2016-2026) of engineering activity, with tickets distributed across ~450
  engineering employees and following realistic patterns:
  - 75% of tickets are resolved, 25% remain open
  - Story points follow Fibonacci sequence (1,2,3,5,8,13)
  - Sprint names use ISO week format (e.g. "Sprint 2024-W03")
  - Titles are generated from realistic templates by ticket type
  - Priority distribution favors Medium/Low over Critical/High
  - Assignee and reporter can be the same person (self-assigned work)

  Generated deterministically with seed=42 for reproducible synthetic data.
connection: bruin-playground-eu
tags:
  - domain:engineering
  - data_type:fact_table
  - sensitivity:internal
  - pipeline_role:raw
  - update_pattern:snapshot
  - source:synthetic

materialization:
  type: table
  strategy: create+replace
image: python:3.11


columns:
  - name: ticket_key
    type: INTEGER
    description: Unique sequential ticket identifier (1-8000), serves as primary key for the ticket
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: ticket_code
    type: VARCHAR
    description: Human-readable ticket code following ENG-NNNN format (e.g. "ENG-0001", "ENG-7999"), used for external references
    checks:
      - name: not_null
  - name: Title
    type: VARCHAR
    description: Descriptive ticket title generated from templates by ticket type, includes component names and actions (15-45 chars)
    checks:
      - name: not_null
  - name: ticket_type
    type: VARCHAR
    description: |
      Type of engineering work represented by this ticket:
      - Bug: Defect fixes and error resolution
      - Story: New features and user-facing functionality
      - Task: Maintenance, documentation, and operational work
      - Improvement: Performance optimization and enhancements
    checks:
      - name: accepted_values
        value: [Bug, Story, Task, Improvement]
  - name: assignee_employee_key
    type: INTEGER
    description: Foreign key to contoso_raw.employees table, identifies the engineer responsible for completing this ticket (range 1201-1650)
    checks:
      - name: not_null
  - name: reporter_employee_key
    type: INTEGER
    description: Foreign key to contoso_raw.employees table, identifies the engineer who created/reported this ticket (range 1201-1650)
    checks:
      - name: not_null
  - name: priority
    type: VARCHAR
    description: |
      Business priority level determining urgency and scheduling:
      - Critical: Immediate attention required, blocking issues
      - High: Important, should be addressed soon
      - Medium: Normal priority, planned work
      - Low: Nice-to-have, can be deferred
    checks:
      - name: accepted_values
        value: [Critical, High, Medium, Low]
  - name: status
    type: VARCHAR
    description: |
      Current workflow status of the ticket:
      - To Do: Backlog item, not yet started
      - In Progress: Currently being worked on
      - Done: Work completed, ticket resolved
      - Closed: Administratively closed, may include resolved tickets
    checks:
      - name: accepted_values
        value: [To Do, In Progress, Done, Closed]
  - name: story_points
    type: INTEGER
    description: Agile story point estimate following Fibonacci sequence (1,2,3,5,8,13), represents relative complexity/effort
    checks:
      - name: not_null
      - name: accepted_values
        value: [1, 2, 3, 5, 8, 13]
  - name: sprint_name
    type: VARCHAR
    description: Sprint identifier using format "Sprint YYYY-WNN" based on ISO week of CreatedDate (e.g. "Sprint 2024-W03")
    checks:
      - name: not_null
  - name: created_date
    type: DATE
    description: Date when the ticket was originally created/reported, used for sprint assignment and aging analysis
    checks:
      - name: not_null
  - name: resolved_date
    type: DATE
    description: Date when ticket was marked as resolved/completed, null for open tickets (~25% of dataset)
  - name: department_key
    type: INTEGER
    description: Foreign key to contoso_raw.departments table, always 6 (Engineering department) as tickets are scoped to engineering
    checks:
      - name: not_null
      - name: accepted_values
        value: [6]
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this data was generated/loaded, used for data lineage and freshness tracking
    checks:
      - name: not_null

@bruin"""

import logging
import os
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _contoso_helpers import load_contoso_keys, seed_all

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

NUM_TICKETS = 8000
ENGINEERING_DEPT_KEY = 6

TICKET_TYPES = ["Bug", "Story", "Task", "Improvement"]
PRIORITIES = ["Critical", "High", "Medium", "Low"]
STORY_POINTS = [1, 2, 3, 5, 8, 13]

TITLE_TEMPLATES = {
    "Bug": [
        "Fix {component} crash on {action}",
        "Resolve {component} timeout issue",
        "{component} returns incorrect {entity} data",
        "Memory leak in {component} service",
        "Fix {component} validation error",
    ],
    "Story": [
        "Implement {component} {feature}",
        "Add {feature} to {component}",
        "Build {component} dashboard {feature}",
        "Create {component} API endpoint for {entity}",
    ],
    "Task": [
        "Update {component} dependencies",
        "Migrate {component} to new {feature}",
        "Add monitoring for {component}",
        "Document {component} {feature}",
        "Refactor {component} {entity} logic",
    ],
    "Improvement": [
        "Optimize {component} {action} performance",
        "Improve {component} error handling",
        "Enhance {component} {feature} UX",
        "Reduce {component} latency for {action}",
    ],
}

COMPONENTS = ["Checkout", "Inventory", "Payment", "Search", "Auth", "Cart",
              "Catalog", "Shipping", "Analytics", "Notification", "Order", "CRM"]
FEATURES = ["filtering", "caching", "pagination", "export", "import", "sync",
            "validation", "logging", "retry logic", "batch processing"]
ACTIONS = ["login", "search", "checkout", "page load", "data sync", "export"]
ENTITIES = ["product", "order", "customer", "invoice", "shipment", "user"]


def materialize():
    seed_all(42)
    rng = np.random.default_rng(42)
    keys = load_contoso_keys()
    min_date, max_date = keys["date_range"]

    # Engineering employees: keys 1-3000, dept 6 starts around emp 1200
    # Simplified: use employee keys in range that maps to engineering
    eng_emp_keys = list(range(1201, 1651))  # ~450 engineering employees

    logger.info("Generating %d sprint tickets...", NUM_TICKETS)

    records = []
    for i in range(NUM_TICKETS):
        ticket_key = i + 1
        ticket_code = f"ENG-{ticket_key:04d}"

        ticket_type = rng.choice(TICKET_TYPES, p=[0.25, 0.35, 0.25, 0.15])
        priority = rng.choice(PRIORITIES, p=[0.05, 0.20, 0.50, 0.25])
        points = int(rng.choice(STORY_POINTS, p=[0.15, 0.25, 0.25, 0.20, 0.10, 0.05]))

        # Generate title
        templates = TITLE_TEMPLATES[ticket_type]
        template = rng.choice(templates)
        title = template.format(
            component=rng.choice(COMPONENTS),
            feature=rng.choice(FEATURES),
            action=rng.choice(ACTIONS),
            entity=rng.choice(ENTITIES),
        )

        assignee = int(rng.choice(eng_emp_keys))
        reporter = int(rng.choice(eng_emp_keys))

        days_range = (max_date - min_date).days - 30
        offset = int(rng.integers(0, max(1, days_range)))
        created_date = (min_date + timedelta(days=offset)).date()

        # Sprint name based on ISO week
        dt = pd.Timestamp(created_date)
        sprint_name = f"Sprint {dt.isocalendar()[0]}-W{dt.isocalendar()[1]:02d}"

        # 75% resolved
        resolved_date = None
        if rng.random() < 0.75:
            cycle_days = int(rng.integers(1, 30))
            resolved_date = created_date + timedelta(days=cycle_days)
            status = rng.choice(["Done", "Closed"], p=[0.8, 0.2])
        else:
            status = rng.choice(["In Progress", "To Do"], p=[0.4, 0.6])

        records.append({
            "TicketKey": ticket_key,
            "TicketCode": ticket_code,
            "Title": title,
            "TicketType": ticket_type,
            "AssigneeEmployeeKey": assignee,
            "ReporterEmployeeKey": reporter,
            "Priority": priority,
            "Status": status,
            "StoryPoints": points,
            "SprintName": sprint_name,
            "CreatedDate": created_date,
            "ResolvedDate": resolved_date,
            "DepartmentKey": ENGINEERING_DEPT_KEY,
        })

    df = pd.DataFrame(records)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d sprint tickets", len(df))
    return df
