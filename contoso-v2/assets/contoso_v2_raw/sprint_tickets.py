"""@bruin

name: contoso_v2_raw.sprint_tickets
description: |
  Engineering sprint tickets for Contoso v2, scaled to the engineering team
  growth: ~10 engineers (2015 first eng hire) → ~450 engineers (2026).
  Sparse ticket_key with deletes (gaps). Story-point distribution shifts
  from Scrum-heavy 1/2/3/5 (2015-2018) to T-shirt-style with more 8s/13s
  (2020+). Bug ratio spikes 2020-Q2 (remote-work transition) and 2023-Q1
  (AI launch). Cycle time correlates to priority and team load. ~25% remain
  open. Tickets are scoped to Engineering (department_key=6); only
  engineering employees can be assignees/reporters.
connection: gcp-default
tags:
  - engineering
  - fact_table

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: ticket_key
    type: INTEGER
    primary_key: true
    description: Sparse ticket identifier.
    checks:
      - name: not_null
      - name: unique
  - name: ticket_code
    type: VARCHAR
    description: ENG-NNNN code.
    checks:
      - name: not_null
  - name: title
    type: VARCHAR
    description: Ticket title.
    checks:
      - name: not_null
  - name: ticket_type
    type: VARCHAR
    description: Bug, Story, Task, Improvement.
    checks:
      - name: not_null
      - name: accepted_values
        value: [Bug, Story, Task, Improvement]
  - name: assignee_employee_key
    type: INTEGER
    description: Foreign key to employees (Engineering only).
  - name: reporter_employee_key
    type: INTEGER
    description: Foreign key to employees (Engineering only).
  - name: priority
    type: VARCHAR
    description: Critical, High, Medium, Low.
    checks:
      - name: accepted_values
        value: [Critical, High, Medium, Low]
  - name: status
    type: VARCHAR
    description: To Do, In Progress, Done, Closed.
    checks:
      - name: accepted_values
        value: [To Do, In Progress, Done, Closed]
  - name: story_points
    type: INTEGER
    description: Fibonacci 1/2/3/5/8/13.
    checks:
      - name: accepted_values
        value: [1, 2, 3, 5, 8, 13]
  - name: sprint_name
    type: VARCHAR
    description: Sprint identifier (Sprint YYYY-WNN).
    checks:
      - name: not_null
  - name: created_date
    type: DATE
    description: Date the ticket was created.
    checks:
      - name: not_null
  - name: resolved_date
    type: DATE
    description: Date the ticket was resolved (NULL for open).
  - name: cycle_time_days
    type: INTEGER
    description: Days from created to resolved (NULL for open).
  - name: department_key
    type: INTEGER
    description: Always 6 (Engineering).
    checks:
      - name: not_null
      - name: accepted_values
        value: [6]
  - name: extracted_at
    type: TIMESTAMP

@bruin"""

import logging
import os
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd

import sys; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _contoso_helpers import (
    PIPELINE_END, build_employees, get_seeded_rng, sparse_keys,
)

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)

ENG_DEPT_KEY = 6

TICKET_TYPES = ["Bug", "Story", "Task", "Improvement"]
PRIORITIES = ["Critical", "High", "Medium", "Low"]

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
              "Catalog", "Shipping", "Analytics", "Notification", "Order",
              "CRM", "AI Recommender", "Pricing", "Recommendations"]
FEATURES = ["filtering", "caching", "pagination", "export", "import", "sync",
            "validation", "logging", "retry logic", "batch processing",
            "ranker", "embedding lookup"]
ACTIONS = ["login", "search", "checkout", "page load", "data sync", "export"]
ENTITIES = ["product", "order", "customer", "invoice", "shipment", "user"]


def _yearly_ticket_count(year: int) -> int:
    """Tickets per year, scaling with engineering team size."""
    base = {
        2015: 200, 2016: 350, 2017: 500, 2018: 650, 2019: 800,
        2020: 1100, 2021: 1300, 2022: 1500, 2023: 2200, 2024: 2700,
        2025: 2400, 2026: 1000,  # partial year
    }
    return base.get(year, 0)


def _bug_ratio(d: date) -> float:
    """Bug ratio with 2020-Q2 and 2023-Q1 spikes."""
    base = 0.25
    if d.year == 2020 and d.month in (3, 4, 5, 6):
        return 0.45
    if d.year == 2023 and d.month in (1, 2, 3):
        return 0.40
    return base


def _story_point_dist(year: int) -> tuple[list[int], np.ndarray]:
    """Distribution shifts toward 8/13 in 2020+."""
    if year <= 2018:
        # Scrum-heavy 1/2/3/5
        return [1, 2, 3, 5, 8, 13], np.array([0.20, 0.30, 0.25, 0.18, 0.05, 0.02])
    elif year <= 2020:
        return [1, 2, 3, 5, 8, 13], np.array([0.15, 0.25, 0.25, 0.20, 0.10, 0.05])
    else:
        return [1, 2, 3, 5, 8, 13], np.array([0.10, 0.18, 0.22, 0.22, 0.18, 0.10])


def materialize():
    rng = get_seeded_rng(167)
    employees = build_employees()
    eng = employees[employees["department_key"] == ENG_DEPT_KEY].copy()
    eng_keys = eng["employee_key"].tolist()
    eng_hire = pd.to_datetime(eng["hire_date"]).dt.date.tolist()
    eng_term = [
        d.date() if pd.notna(d) else None
        for d in pd.to_datetime(eng["termination_date"])
    ]

    if not eng_keys:
        # If no engineering staff yet, can't generate tickets
        logger.warning("No engineering employees found; skipping sprint_tickets")
        return pd.DataFrame()

    sparse_iter = iter(sparse_keys(40_000, gap_rate=0.16, start=1, seed=167))

    records = []
    for year in range(2015, 2027):
        n = _yearly_ticket_count(year)
        if n == 0:
            continue
        for _ in range(n):
            month = int(rng.integers(1, 13))
            day = int(rng.integers(1, 28))
            try:
                created_date = date(year, month, day)
            except ValueError:
                created_date = date(year, month, 15)
            if created_date > PIPELINE_END:
                continue

            # Eligible engineers on this date
            eligible_idx = [
                i for i, hd in enumerate(eng_hire)
                if hd <= created_date and (eng_term[i] is None or eng_term[i] >= created_date)
            ]
            if not eligible_idx:
                continue

            # Type
            if rng.random() < _bug_ratio(created_date):
                tt = "Bug"
            else:
                tt = str(rng.choice(["Story", "Task", "Improvement"],
                                    p=[0.50, 0.30, 0.20]))

            # Priority — bugs lean higher, others flat
            if tt == "Bug":
                priority = str(rng.choice(PRIORITIES, p=[0.10, 0.30, 0.45, 0.15]))
            else:
                priority = str(rng.choice(PRIORITIES, p=[0.03, 0.18, 0.55, 0.24]))

            # Story points
            sp_choices, sp_weights = _story_point_dist(year)
            sp_weights = sp_weights / sp_weights.sum()
            points = int(rng.choice(sp_choices, p=sp_weights))

            template = str(rng.choice(TITLE_TEMPLATES[tt]))
            title = template.format(
                component=str(rng.choice(COMPONENTS)),
                feature=str(rng.choice(FEATURES)),
                action=str(rng.choice(ACTIONS)),
                entity=str(rng.choice(ENTITIES)),
            )

            assignee = int(eng_keys[int(rng.choice(eligible_idx))])
            reporter = int(eng_keys[int(rng.choice(eligible_idx))])

            ticket_key = int(next(sparse_iter))
            ticket_code = f"ENG-{ticket_key:05d}"

            iso = pd.Timestamp(created_date).isocalendar()
            sprint_name = f"Sprint {iso[0]}-W{iso[1]:02d}"

            # ~75% resolved; cycle time correlated to priority + points
            base_cycle = {"Critical": 2, "High": 5, "Medium": 12, "Low": 25}[priority]
            base_cycle += int(points * 1.5)

            resolved_date = None
            cycle_days = None
            if rng.random() < 0.75:
                cycle_days = max(1, int(rng.gamma(shape=2.5, scale=base_cycle / 2.5)))
                resolved_date = created_date + timedelta(days=cycle_days)
                if resolved_date > PIPELINE_END:
                    resolved_date = None
                    cycle_days = None
                    status = "In Progress"
                else:
                    status = str(rng.choice(["Done", "Closed"], p=[0.85, 0.15]))
            else:
                status = str(rng.choice(["In Progress", "To Do"], p=[0.45, 0.55]))

            records.append({
                "ticket_key": ticket_key,
                "ticket_code": ticket_code,
                "title": title,
                "ticket_type": tt,
                "assignee_employee_key": assignee,
                "reporter_employee_key": reporter,
                "priority": priority,
                "status": status,
                "story_points": points,
                "sprint_name": sprint_name,
                "created_date": created_date,
                "resolved_date": resolved_date,
                "cycle_time_days": cycle_days,
                "department_key": ENG_DEPT_KEY,
            })

    df = pd.DataFrame(records)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d sprint tickets", len(df))
    return df
