"""@bruin

name: contoso_raw.employees
description: |
  Comprehensive employee master data for Contoso's consumer electronics retailer,
  containing 3,000 synthetic employees distributed across 12 departments following
  realistic organizational patterns. This dataset simulates a multi-national workforce
  with employees spanning Executive (2%), Sales (35%), Marketing (8%), Finance (5%),
  HR (4%), Engineering (15%), Customer Support (10%), Operations (8%), Legal (3%),
  Product (5%), Facilities (2%), and Data & Analytics (3%) departments.

  The data models realistic employment patterns including:
  - Hierarchical management structure with 15.6% senior employees having no direct manager
  - Store-based employees (55%) primarily in Sales, Customer Support, Operations, and Facilities
  - Salary bands aligned to job levels (L1: $35K-$55K through L5: $200K-$350K)
  - 15% termination rate with terminated employees having end dates
  - Corporate email pattern: firstname.lastname@contoso.com
  - Deterministic generation using seed=42 for reproducible testing

  Key relationships: Links to departments (department_key), stores (store_key for retail roles),
  and self-referencing manager hierarchy. Supports cross-departmental analytics including
  payroll analysis, organizational reporting, workforce planning, and compensation benchmarking.
connection: bruin-playground-eu
tags:
  - domain:hr
  - domain:workforce
  - data_type:dimension_table
  - data_type:master_data
  - sensitivity:pii
  - pipeline_role:raw
  - update_pattern:slowly_changing
  - synthetic_data

materialization:
  type: table
  strategy: create+replace
image: python:3.11


columns:
  - name: employee_key
    type: INTEGER
    description: |
      Primary identifier for employees, sequential integers 1-3000. Used as foreign key
      in payroll, manager_key self-references, and other employee-related datasets.
      Generated deterministically to ensure consistent cross-table joins.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: first_name
    type: VARCHAR
    description: |
      Employee first name generated via Faker library. 524 distinct values across
      3000 employees with realistic name distribution. Length ranges 2-11 characters.
      Used in email generation and employee directory displays.
    checks:
      - name: not_null
  - name: last_name
    type: VARCHAR
    description: |
      Employee last name generated via Faker library. 808 distinct values providing
      realistic surname diversity. Length ranges 2-11 characters. Combined with
      first_name to create corporate email addresses.
    checks:
      - name: not_null
  - name: email
    type: VARCHAR
    description: |
      Corporate email address following pattern "firstname.lastname@contoso.com".
      Nearly unique (2938/3000 distinct) with occasional duplicates due to common
      name combinations. Length ranges 18-33 characters. Primary communication
      identifier for employees.
    checks:
      - name: not_null
  - name: hire_date
    type: DATE
    description: |
      Date employee joined Contoso, spanning 2008-01-02 to 2025-05-29 to simulate
      long-tenured and recent hires. No null values. Used for tenure calculations,
      anniversary tracking, and workforce growth analysis. Always precedes or equals
      termination_date when present.
    checks:
      - name: not_null
  - name: termination_date
    type: DATE
    description: |
      Employment end date for terminated employees. Null for 85.6% of employees (active),
      with dates ranging 2009-01-07 to 2025-12-25. 407 distinct termination dates
      across 433 terminated employees. Used for turnover analysis and workforce planning.
  - name: department_key
    type: INTEGER
    description: |
      Foreign key reference to departments table (1-12). All employees assigned to
      exactly one department with realistic distribution: Sales (35%), Engineering (15%),
      Customer Support (10%), Operations (8%), Marketing (8%), Finance (5%), HR (4%),
      Product (5%), Legal (3%), Facilities (2%), Data & Analytics (3%), Executive (2%).
    checks:
      - name: not_null
  - name: store_key
    type: INTEGER
    description: |
      Foreign key to stores table for retail-facing employees. Null for 45% of workforce
      (corporate/remote roles). Non-null values link to 74 physical store locations for
      Sales, Customer Support, Operations, and Facilities departments. Essential for
      location-based workforce analysis and store staffing optimization.
  - name: job_title
    type: VARCHAR
    description: |
      Specific role title within department and level, e.g., "Senior Software Engineer",
      "Regional Sales Lead", "Marketing Coordinator". 90 distinct titles across all
      departments and levels. Length ranges 3-28 characters. Reflects realistic
      job progression and organizational structure.
    checks:
      - name: not_null
  - name: level
    type: VARCHAR
    description: |
      Standardized job level classification L1-L5 representing career progression.
      Distribution: L1 (35%), L2 (30%), L3 (20%), L4 (10%), L5 (5%) following
      typical organizational pyramid. Fixed 2-character format. Determines salary
      bands and reporting relationships.
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
    description: |
      Self-referencing foreign key to employee_key indicating direct manager.
      Null for 15.6% of employees (senior leadership with no manager). Creates
      hierarchical reporting structure with L3+ employees typically managing others.
      Range 62-2998 reflects realistic management distribution across departments.
  - name: salary
    type: DOUBLE
    description: |
      Annual base salary in USD, ranges $35,010-$349,538 aligned to job levels.
      No null values. Level-based bands: L1 ($35K-$55K), L2 ($55K-$85K),
      L3 ($85K-$130K), L4 ($130K-$200K), L5 ($200K-$350K). Used for compensation
      analysis, budget planning, and pay equity reporting.
    checks:
      - name: not_null
  - name: currency
    type: VARCHAR
    description: |
      Salary currency code, always "USD" for all employees. 3-character ISO code.
      Consistent single currency simplifies financial reporting and cross-department
      compensation comparisons. Future international expansion may introduce additional currencies.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - USD
  - name: status
    type: VARCHAR
    description: |
      Current employment status indicating whether employee is actively employed.
      Binary values: "Active" (85.6%) for current employees, "Terminated" (14.4%)
      for former employees. Critical for payroll processing, access control, and
      workforce counting. Terminated employees retain historical data for analytics.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Active
          - Terminated
  - name: extracted_at
    type: TIMESTAMP
    description: |
      ETL metadata timestamp indicating when this dataset was generated and loaded
      into BigQuery (UTC). Identical across all records since this is a synthetic
      batch-generated dataset. Used for data lineage tracking and debugging pipeline issues.
    checks:
      - name: not_null

@bruin"""

import logging
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd

import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _contoso_helpers import get_seeded_faker, load_contoso_keys, seed_all

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# (DepartmentKey, headcount_pct, job_titles_by_level)
DEPT_CONFIG = {
    1: (0.02, {  # Executive
        "L5": ["CEO", "CFO", "CTO", "COO", "CMO", "CHRO"],
        "L4": ["VP Strategy", "VP Corporate Development"],
    }),
    2: (0.35, {  # Sales
        "L1": ["Sales Associate", "Retail Clerk"],
        "L2": ["Senior Sales Associate", "Sales Specialist"],
        "L3": ["Store Manager", "Regional Sales Lead"],
        "L4": ["Regional Director", "Director of Sales"],
        "L5": ["VP Sales"],
    }),
    3: (0.08, {  # Marketing
        "L1": ["Marketing Coordinator", "Content Writer"],
        "L2": ["Marketing Specialist", "Campaign Manager"],
        "L3": ["Senior Marketing Manager", "Brand Manager"],
        "L4": ["Director of Marketing"],
        "L5": ["VP Marketing"],
    }),
    4: (0.05, {  # Finance
        "L1": ["Accountant", "Financial Analyst"],
        "L2": ["Senior Accountant", "Senior Financial Analyst"],
        "L3": ["Finance Manager", "Controller"],
        "L4": ["Director of Finance"],
        "L5": ["VP Finance"],
    }),
    5: (0.04, {  # Human Resources
        "L1": ["HR Coordinator", "Recruiter"],
        "L2": ["HR Specialist", "Senior Recruiter"],
        "L3": ["HR Manager", "Talent Acquisition Manager"],
        "L4": ["Director of HR"],
        "L5": ["VP Human Resources"],
    }),
    6: (0.15, {  # Engineering
        "L1": ["Junior Software Engineer", "QA Analyst"],
        "L2": ["Software Engineer", "DevOps Engineer", "QA Engineer"],
        "L3": ["Senior Software Engineer", "Staff Engineer", "Senior QA Engineer"],
        "L4": ["Engineering Manager", "Principal Engineer"],
        "L5": ["VP Engineering"],
    }),
    7: (0.10, {  # Customer Support
        "L1": ["Support Agent", "Support Representative"],
        "L2": ["Senior Support Agent", "Technical Support Specialist"],
        "L3": ["Support Team Lead", "Support Manager"],
        "L4": ["Director of Customer Support"],
    }),
    8: (0.08, {  # Operations
        "L1": ["Warehouse Associate", "Logistics Coordinator"],
        "L2": ["Warehouse Supervisor", "Supply Chain Analyst"],
        "L3": ["Operations Manager", "Logistics Manager"],
        "L4": ["Director of Operations"],
        "L5": ["VP Operations"],
    }),
    9: (0.03, {  # Legal
        "L2": ["Paralegal", "Compliance Analyst"],
        "L3": ["Corporate Counsel", "Senior Compliance Officer"],
        "L4": ["General Counsel"],
    }),
    10: (0.05, {  # Product
        "L1": ["Associate Product Manager"],
        "L2": ["Product Manager", "Product Analyst"],
        "L3": ["Senior Product Manager", "Group Product Manager"],
        "L4": ["Director of Product"],
        "L5": ["VP Product"],
    }),
    11: (0.02, {  # Facilities
        "L1": ["Maintenance Technician", "Facilities Coordinator"],
        "L2": ["Facilities Specialist"],
        "L3": ["Facilities Manager"],
    }),
    12: (0.03, {  # Data & Analytics
        "L1": ["Data Analyst", "BI Analyst"],
        "L2": ["Senior Data Analyst", "Data Engineer"],
        "L3": ["Senior Data Engineer", "Analytics Manager"],
        "L4": ["Director of Data & Analytics"],
    }),
}

SALARY_RANGES = {
    "L1": (35_000, 55_000),
    "L2": (55_000, 85_000),
    "L3": (85_000, 130_000),
    "L4": (130_000, 200_000),
    "L5": (200_000, 350_000),
}

LEVEL_DISTRIBUTION = {"L1": 0.35, "L2": 0.30, "L3": 0.20, "L4": 0.10, "L5": 0.05}

TOTAL_EMPLOYEES = 3000


def materialize():
    seed_all(42)
    fake = get_seeded_faker(42)
    rng = np.random.default_rng(42)
    keys = load_contoso_keys()
    store_keys = keys["store_keys"]

    logger.info("Generating %d employees...", TOTAL_EMPLOYEES)

    records = []
    manager_pool = {}  # dept_key -> list of employee_keys at L3+

    for dept_key, (pct, titles_by_level) in DEPT_CONFIG.items():
        n = max(1, int(TOTAL_EMPLOYEES * pct))
        available_levels = list(titles_by_level.keys())

        for i in range(n):
            emp_key = len(records) + 1

            # pick level based on distribution, filtered to what dept offers
            level_weights = [LEVEL_DISTRIBUTION.get(lv, 0) for lv in available_levels]
            total_w = sum(level_weights)
            level_weights = [w / total_w for w in level_weights]
            level = rng.choice(available_levels, p=level_weights)

            titles = titles_by_level[level]
            job_title = rng.choice(titles)

            sal_min, sal_max = SALARY_RANGES[level]
            salary = round(rng.uniform(sal_min, sal_max), 2)

            hire_date = fake.date_between(
                start_date=datetime(2008, 1, 1), end_date=datetime(2025, 6, 1)
            )

            terminated = rng.random() < 0.15
            term_date = None
            status = "Active"
            if terminated:
                term_date = fake.date_between(start_date=hire_date, end_date=datetime(2025, 12, 31))
                status = "Terminated"

            # Sales and Operations employees get assigned to stores
            store_key = None
            if dept_key in (2, 7, 8, 11):
                store_key = int(rng.choice(store_keys))

            first = fake.first_name()
            last = fake.last_name()
            email = f"{first.lower()}.{last.lower()}@contoso.com"

            records.append({
                "EmployeeKey": emp_key,
                "FirstName": first,
                "LastName": last,
                "Email": email,
                "HireDate": hire_date,
                "TerminationDate": term_date,
                "DepartmentKey": dept_key,
                "StoreKey": store_key,
                "JobTitle": job_title,
                "Level": level,
                "ManagerKey": None,  # set below
                "Salary": salary,
                "Currency": "USD",
                "Status": status,
            })

            if level in ("L3", "L4", "L5"):
                manager_pool.setdefault(dept_key, []).append(emp_key)

    # Assign managers: L1-L2 report to L3+, L3 to L4+, L4 to L5
    for rec in records:
        dept = rec["DepartmentKey"]
        level = rec["Level"]
        candidates = [
            m for m in manager_pool.get(dept, [])
            if m != rec["EmployeeKey"]
        ]
        if candidates and level in ("L1", "L2", "L3"):
            rec["ManagerKey"] = int(rng.choice(candidates))

    df = pd.DataFrame(records)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d employees across %d departments", len(df), len(DEPT_CONFIG))
    return df
