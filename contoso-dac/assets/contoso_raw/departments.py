"""@bruin

name: contoso_raw.departments
description: |
  Contoso organizational hierarchy representing the consumer electronics retailer's
  12 business units in a hierarchical tree structure. Executive serves as the root
  department (parent_department_key=null), with Sales, Marketing, Finance, HR,
  Engineering, Operations, and Legal as direct reports. Engineering has two
  sub-departments (Product, Data & Analytics), Sales has Customer Support, and
  Operations includes Facilities. Each department has a unique cost center code
  following the pattern CC-XXX for accounting and budget allocation purposes.

  This synthetic dataset provides the foundation for employee assignments across
  ~3,000 employees and supports cross-departmental analytics including payroll,
  budget allocation, and organizational reporting. The hierarchy enables rollup
  calculations for management reporting and departmental KPIs.
connection: contoso-duckdb
tags:
  - domain:hr
  - domain:organizational
  - data_type:dimension_table
  - data_type:master_data
  - sensitivity:internal
  - pipeline_role:raw
  - update_pattern:static
  - synthetic_data

materialization:
  type: table
  strategy: create+replace
image: python:3.11


columns:
  - name: department_key
    type: INTEGER
    description: |
      Primary identifier for each department. Sequential integers 1-12 representing
      unique business units within Contoso's organizational structure. Referenced
      as foreign key by employees, payroll, budgets, and other departmental data.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: department_name
    type: VARCHAR
    description: |
      Human-readable department name (e.g., "Executive", "Engineering", "Data & Analytics").
      Business unit names that align with Contoso's functional areas. All 12 values
      are unique and range from 5-16 characters in length.
    checks:
      - name: not_null
      - name: unique
  - name: cost_center
    type: VARCHAR
    description: |
      Accounting cost center code in format "CC-XXX" where XXX is a 3-4 digit number.
      Used for budget allocation, expense tracking, and financial reporting. Each
      department has exactly one cost center for P&L attribution and cross-charging.
    checks:
      - name: not_null
      - name: unique
  - name: parent_department_key
    type: INTEGER
    description: |
      Foreign key reference to department_key indicating hierarchical reporting
      structure. Null only for Executive (top-level department). Most departments
      report to Executive (key=1), with some sub-departments like Product (reports
      to Engineering) and Customer Support (reports to Sales) creating a 2-level hierarchy.
  - name: extracted_at
    type: TIMESTAMP
    description: |
      ETL metadata timestamp indicating when this static dataset was loaded into
      the warehouse (UTC). All records share the same extraction time since this
      is a fixed synthetic dataset that doesn't change between pipeline runs.
    checks:
      - name: not_null

@bruin"""

import logging
import os
from datetime import datetime, timezone

import pandas as pd

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEPARTMENTS = [
    (1, "Executive", "CC-100", None),
    (2, "Sales", "CC-200", 1),
    (3, "Marketing", "CC-300", 1),
    (4, "Finance", "CC-400", 1),
    (5, "Human Resources", "CC-500", 1),
    (6, "Engineering", "CC-600", 1),
    (7, "Customer Support", "CC-700", 2),
    (8, "Operations", "CC-800", 1),
    (9, "Legal", "CC-900", 1),
    (10, "Product", "CC-1000", 6),
    (11, "Facilities", "CC-1100", 8),
    (12, "Data & Analytics", "CC-1200", 6),
]


def materialize():
    logger.info("Generating departments...")
    df = pd.DataFrame(
        DEPARTMENTS,
        columns=["DepartmentKey", "DepartmentName", "CostCenter", "ParentDepartmentKey"],
    )
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d departments", len(df))
    return df
