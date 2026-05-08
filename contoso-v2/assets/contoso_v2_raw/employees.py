"""@bruin

name: contoso_v2_raw.employees
description: |
  Employee master for Contoso v2. Headcount grows from ~60 in 2010 to ~3,200 by
  2024, with attrition ~8% baseline, ~12% in 2020 (COVID layoffs concentrated in
  April), and ~15% in 2022 (Great Resignation). Hiring biased to July-August
  (back-to-school) and Oct-Nov (holiday) for retail-facing departments; near-zero
  March-August 2020 (COVID freeze); 2.5x spike 2023-2024 (AI boom hiring). Sparse
  employee_key reflects gaps from rehires/voids. Cohort effects: 2020+ hires get
  ~8% salary bump. ~1.5% data quality issues (NULL manager, malformed email).
connection: gcp-default
tags:
  - dimension_table
  - master_data
  - hr
  - sensitivity:pii

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: employee_key
    type: INTEGER
    primary_key: true
    description: Employee primary key (sparse integers).
    checks:
      - name: not_null
      - name: unique
  - name: first_name
    type: VARCHAR
    description: Employee first name.
    checks:
      - name: not_null
  - name: last_name
    type: VARCHAR
    description: Employee last name.
    checks:
      - name: not_null
  - name: email
    type: VARCHAR
    description: Corporate email (1.5% may be malformed — data quality).
    checks:
      - name: not_null
  - name: hire_date
    type: DATE
    description: Date employee joined Contoso.
    checks:
      - name: not_null
  - name: termination_date
    type: DATE
    description: Date employee left (NULL if still active).
  - name: department_key
    type: INTEGER
    description: Foreign key to departments.
    checks:
      - name: not_null
  - name: store_key
    type: INTEGER
    description: Store assignment for retail-facing roles (NULL for corporate).
  - name: job_title
    type: VARCHAR
    description: Job title.
    checks:
      - name: not_null
  - name: level
    type: VARCHAR
    description: Career level L1-L5.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - L1
          - L2
          - L3
          - L4
          - L5
  - name: manager_key
    type: INTEGER
    description: Self-reference to manager's employee_key (NULL for senior leadership and ~1.5% data quality nulls).
  - name: salary
    type: NUMERIC
    description: Annual base salary in USD.
    checks:
      - name: not_null
      - name: non_negative
  - name: currency
    type: VARCHAR
    description: Always USD.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - USD
  - name: status
    type: VARCHAR
    description: Active or Terminated.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Active
          - Terminated
  - name: extracted_at
    type: TIMESTAMP
    description: ETL processing timestamp.

@bruin"""

import logging
import os
from datetime import datetime, timezone

import sys; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _contoso_helpers import build_employees

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)


def materialize():
    df = build_employees().copy()
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d employees", len(df))
    return df
