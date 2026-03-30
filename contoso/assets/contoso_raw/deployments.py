"""@bruin

name: contoso_raw.deployments
description: |
  Comprehensive software deployment tracking for Contoso's consumer electronics platform,
  containing 1,200 synthetic deployment records spanning 15 microservices across production,
  staging, and development environments from 2016-2026.

  This dataset represents the complete deployment lifecycle including successful deployments,
  rollbacks, and failures. It captures Contoso's engineering deployment patterns where 90% of
  deployments succeed, 7% require rollbacks, and 3% fail completely. About 80% of deployments
  are linked to sprint tickets (planned work) while 20% represent hotfixes deployed outside
  the normal sprint cycle.

  The data spans 15 core services including checkout-api, inventory-service, payment-gateway,
  search-service, auth-service, cart-service, catalog-api, shipping-service, analytics-pipeline,
  notification-service, order-service, crm-api, web-frontend, mobile-bff, and admin-dashboard.

  Key relationships: Links to employees table (deployed_by) for engineering team members
  (employee_key range 1201-1650) and sprint_tickets table (ticket_key) for planned work tracking.
  Supports deployment frequency analysis, failure rate monitoring, rollback pattern analysis,
  and engineering velocity measurement.

  Generated deterministically with seed=42 for reproducible synthetic data modeling realistic
  deployment cadences, failure patterns, and operational characteristics.
connection: gcp-default
tags:
  - domain:engineering
  - domain:devops
  - data_type:fact_table
  - sensitivity:internal
  - pipeline_role:raw
  - update_pattern:snapshot
  - source:synthetic
  - operational_data

materialization:
  type: table
  strategy: create+replace
image: python:3.11


columns:
  - name: deployment_key
    type: INTEGER
    description: |
      Primary identifier for deployments, sequential integers from 1-1200. Serves as the
      natural key for joining with other engineering datasets and tracking deployment
      sequences over time.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: service_name
    type: VARCHAR
    description: |
      Name of the deployed microservice. Contains 15 distinct services representing Contoso's
      modular architecture: checkout-api, inventory-service, payment-gateway, search-service,
      auth-service, cart-service, catalog-api, shipping-service, analytics-pipeline,
      notification-service, order-service, crm-api, web-frontend, mobile-bff, admin-dashboard.
      Used for service-level deployment frequency and reliability analysis.
    checks:
      - name: not_null
  - name: environment
    type: VARCHAR
    description: |
      Target deployment environment with realistic distribution reflecting promotion patterns:
      40% Production, 35% Staging, 25% Development. Critical for environment-specific deployment
      analysis, promotion tracking, and risk assessment. Production deployments require higher
      scrutiny and have different failure tolerances.
    checks:
      - name: not_null
  - name: deployed_by
    type: INTEGER
    description: |
      Foreign key to employees table (employee_key) identifying the engineer who executed
      the deployment. References engineering team members with employee_key in range 1201-1650.
      Enables deployment workload analysis, individual engineer deployment patterns, and
      responsibility tracking for deployment outcomes.
    checks:
      - name: not_null
  - name: deploy_date
    type: DATE
    description: |
      Date when the deployment was executed. Spans 2016-2026 with 1028 unique deployment
      dates out of 1200 total deployments, indicating realistic clustering of deployments
      on specific days. Critical for deployment frequency analysis, release cadence tracking,
      and temporal pattern identification.
    checks:
      - name: not_null
  - name: rollback_date
    type: DATE
    description: |
      Date when deployment was rolled back due to issues, null for successful deployments.
      Only 77 non-null values (6.4% rollback rate) reflecting realistic operational patterns.
      When populated, typically occurs within 0-2 days of original deployment. Used for
      rollback frequency analysis, time-to-rollback metrics, and deployment stability assessment.
  - name: status
    type: VARCHAR
    description: |
      Final deployment outcome following realistic distribution: ~90% Success, ~7% Rolled Back,
      ~3% Failed. Success indicates stable deployment, Rolled Back means deployment was reverted
      due to issues, Failed represents deployments that could not be completed or immediately
      failed. Primary metric for deployment reliability and engineering process effectiveness.
    checks:
      - name: not_null
  - name: ticket_key
    type: FLOAT64
    description: |
      Foreign key to sprint_tickets table (TicketKey) for planned deployments, null for hotfixes.
      About 80% of deployments are linked to tickets (planned work) while 20% represent emergency
      hotfixes deployed outside normal sprint cycles. Range 25-7998 aligns with sprint ticket
      key range. Enables traceability between development work and deployments, planned vs
      unplanned deployment ratio analysis, and sprint delivery tracking.
  - name: extracted_at
    type: TIMESTAMP
    description: |-
      UTC timestamp when deployment data was extracted and loaded into the warehouse. All records
      share the same extraction timestamp as this is batch-loaded synthetic data. Used for
      data lineage tracking, freshness monitoring, and ETL process validation.

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

NUM_DEPLOYMENTS = 1200
SERVICES = [
    "checkout-api", "inventory-service", "payment-gateway", "search-service",
    "auth-service", "cart-service", "catalog-api", "shipping-service",
    "analytics-pipeline", "notification-service", "order-service", "crm-api",
    "web-frontend", "mobile-bff", "admin-dashboard",
]
ENVIRONMENTS = ["Production", "Staging", "Development"]
ENG_EMP_KEYS = list(range(1201, 1651))


def materialize():
    seed_all(42)
    rng = np.random.default_rng(42)
    keys = load_contoso_keys()
    min_date, max_date = keys["date_range"]

    logger.info("Generating %d deployments...", NUM_DEPLOYMENTS)

    records = []
    for i in range(NUM_DEPLOYMENTS):
        deploy_key = i + 1
        service = rng.choice(SERVICES)
        env = rng.choice(ENVIRONMENTS, p=[0.4, 0.35, 0.25])
        deployer = int(rng.choice(ENG_EMP_KEYS))

        days_range = (max_date - min_date).days - 10
        offset = int(rng.integers(0, max(1, days_range)))
        deploy_date = (min_date + timedelta(days=offset)).date()

        # 90% success, 7% rolled back, 3% failed
        roll = rng.random()
        if roll < 0.90:
            status = "Success"
            rollback_date = None
        elif roll < 0.97:
            status = "Rolled Back"
            rollback_date = deploy_date + timedelta(days=int(rng.integers(0, 3)))
        else:
            status = "Failed"
            rollback_date = None

        # 80% linked to a ticket, 20% hotfixes
        ticket_key = None
        if rng.random() < 0.80:
            ticket_key = int(rng.integers(1, 8001))

        records.append({
            "DeploymentKey": deploy_key,
            "ServiceName": service,
            "Environment": env,
            "DeployedBy": deployer,
            "DeployDate": deploy_date,
            "RollbackDate": rollback_date,
            "Status": status,
            "TicketKey": ticket_key,
        })

    df = pd.DataFrame(records)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d deployments", len(df))
    return df
