"""@bruin

name: contoso_v2_raw.deployments
description: |
  Software deployment records for Contoso v2, 2015-01-01 to 2026-05-01.
  Frequency grows ~5x over the decade: ~10/wk in 2015 → ~200/wk in 2025.
  Weekend deploys are rare (5%). Three named "incident weeks" with elevated
  rollback rates: 2020-03 (COVID infra scaling, 30% rollback), 2023-Q1 (AI
  launch, 30%), 2024-Q3 (platform migration, 30%) versus ~3% baseline.
  Status mix: ~92% Success, ~6% Rolled Back, ~2% Failed (baseline).
  ~80% linked to a sprint ticket (planned), ~20% hotfixes. deployed_by
  references real engineering employees active on deploy_date.
connection: gcp-default
tags:
  - engineering
  - devops
  - fact_table

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: deployment_key
    type: INTEGER
    primary_key: true
    description: Sparse deployment identifier.
    checks:
      - name: not_null
      - name: unique
  - name: service_name
    type: VARCHAR
    description: Microservice name.
    checks:
      - name: not_null
  - name: environment
    type: VARCHAR
    description: Production, Staging, or Development.
    checks:
      - name: not_null
      - name: accepted_values
        value: [Production, Staging, Development]
  - name: deployed_by
    type: INTEGER
    description: Foreign key to employees (Engineering only).
  - name: deploy_date
    type: DATE
    description: Date of deployment.
    checks:
      - name: not_null
  - name: rollback_date
    type: DATE
    description: Date the deployment was rolled back (NULL if not rolled back).
  - name: status
    type: VARCHAR
    description: Success, Rolled Back, or Failed.
    checks:
      - name: not_null
      - name: accepted_values
        value: [Success, Rolled Back, Failed]
  - name: ticket_key
    type: INTEGER
    description: Foreign key to sprint_tickets (NULL for hotfixes).
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

SERVICES_2015 = [
    "checkout-api", "inventory-service", "payment-gateway", "search-service",
    "auth-service", "cart-service", "catalog-api", "shipping-service",
    "order-service", "web-frontend",
]
SERVICES_LATER = SERVICES_2015 + [
    "analytics-pipeline", "notification-service", "crm-api",
    "mobile-bff", "admin-dashboard",
]
SERVICES_AI = SERVICES_LATER + [
    "ai-recommender", "embedding-store", "vector-search",
    "personalization-api",
]


def _services_for_year(year: int) -> list[str]:
    if year < 2018:
        return SERVICES_2015
    if year < 2023:
        return SERVICES_LATER
    return SERVICES_AI


def _weekly_deploys(year: int) -> int:
    base = {
        2015: 10, 2016: 18, 2017: 30, 2018: 50, 2019: 70,
        2020: 90, 2021: 110, 2022: 130, 2023: 170, 2024: 200,
        2025: 200, 2026: 180,
    }
    return base.get(year, 50)


def _is_incident_week(d: date) -> bool:
    incidents = [
        (date(2020, 3, 9), date(2020, 3, 22)),
        (date(2023, 1, 23), date(2023, 3, 31)),
        (date(2024, 7, 1), date(2024, 9, 30)),
    ]
    return any(s <= d <= e for s, e in incidents)


def materialize():
    rng = get_seeded_rng(173)
    employees = build_employees()
    eng = employees[employees["department_key"] == ENG_DEPT_KEY].copy()
    eng_keys = eng["employee_key"].tolist()
    eng_hire = pd.to_datetime(eng["hire_date"]).dt.date.tolist()
    eng_term = [
        d.date() if pd.notna(d) else None
        for d in pd.to_datetime(eng["termination_date"])
    ]

    if not eng_keys:
        logger.warning("No engineering employees found; skipping deployments")
        return pd.DataFrame()

    sparse_iter = iter(sparse_keys(60_000, gap_rate=0.10, start=1, seed=173))

    records = []
    cur = date(2015, 1, 1)
    while cur <= PIPELINE_END:
        weekly = _weekly_deploys(cur.year)
        n_today = max(0, int(rng.poisson(weekly / 7.0)))
        # Weekend dampener
        if cur.weekday() >= 5 and rng.random() > 0.05:
            n_today = max(0, int(n_today * 0.10))

        services = _services_for_year(cur.year)

        for _ in range(n_today):
            eligible = [
                i for i, hd in enumerate(eng_hire)
                if hd <= cur and (eng_term[i] is None or eng_term[i] >= cur)
            ]
            if not eligible:
                continue
            deployer = int(eng_keys[int(rng.choice(eligible))])

            service = str(rng.choice(services))
            env = str(rng.choice(["Production", "Staging", "Development"],
                                 p=[0.40, 0.35, 0.25]))

            rollback_p = 0.30 if _is_incident_week(cur) else 0.06
            r = rng.random()
            if r < (1 - rollback_p - 0.02):
                status = "Success"
                rollback_date = None
            elif r < (1 - 0.02):
                status = "Rolled Back"
                rollback_date = cur + timedelta(days=int(rng.integers(0, 3)))
            else:
                status = "Failed"
                rollback_date = None

            ticket_key = None
            if rng.random() < 0.80:
                ticket_key = int(rng.integers(1, 60_000))

            records.append({
                "deployment_key": int(next(sparse_iter)),
                "service_name": service,
                "environment": env,
                "deployed_by": deployer,
                "deploy_date": cur,
                "rollback_date": rollback_date,
                "status": status,
                "ticket_key": ticket_key,
            })
        cur += timedelta(days=1)

    df = pd.DataFrame(records)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d deployments", len(df))
    return df
