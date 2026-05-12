"""@bruin

name: contoso_v2_raw.departments
description: |
  Department dimension for Contoso v2. 12 departments with hierarchical structure
  and `created_date` reflecting when the function was first stood up
  (Engineering/Data/AI in 2015+, others in 2010).
connection: gcp-default
tags:
  - dimension_table
  - reference_data
  - hr

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: department_key
    type: INTEGER
    description: Primary key (1-12).
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: department_name
    type: VARCHAR
    description: Department display name.
    checks:
      - name: not_null
  - name: parent_department_key
    type: INTEGER
    description: Parent department for org hierarchy (NULL for top-level Executive).
  - name: cost_center
    type: VARCHAR
    description: Finance cost center code (CC-NNN).
    checks:
      - name: not_null
  - name: created_date
    type: DATE
    description: Date the department function was first established.
    checks:
      - name: not_null
  - name: extracted_at
    type: TIMESTAMP
    description: ETL processing timestamp.

@bruin"""

import logging
import os
from datetime import date, datetime, timezone

import pandas as pd

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)

DEPARTMENTS = [
    # (key, name, parent, cost_center, created_date)
    (1, "Executive", None, "CC-100", date(2010, 1, 1)),
    (2, "Sales", 1, "CC-200", date(2010, 1, 1)),
    (3, "Marketing", 1, "CC-300", date(2010, 1, 1)),
    (4, "Finance", 1, "CC-400", date(2010, 1, 1)),
    (5, "Human Resources", 1, "CC-500", date(2010, 1, 1)),
    (6, "Engineering", 1, "CC-600", date(2015, 1, 15)),
    (7, "Customer Support", 2, "CC-700", date(2010, 6, 1)),
    (8, "Operations", 1, "CC-800", date(2010, 4, 1)),
    (9, "Legal", 1, "CC-900", date(2012, 3, 1)),
    (10, "Product", 6, "CC-1000", date(2015, 6, 1)),
    (11, "Facilities", 8, "CC-1100", date(2010, 1, 1)),
    (12, "Data & Analytics", 6, "CC-1200", date(2017, 1, 15)),
]


def materialize():
    df = pd.DataFrame(DEPARTMENTS, columns=[
        "department_key", "department_name", "parent_department_key",
        "cost_center", "created_date",
    ])
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d departments", len(df))
    return df
