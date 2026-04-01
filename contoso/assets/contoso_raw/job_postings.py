"""@bruin

name: contoso_raw.job_postings
description: |
  Historical job posting data for Contoso consumer electronics retailer, containing 800
  synthetic job postings spanning 2016-2027 across 12 business departments. This dataset
  models realistic hiring patterns with 75% positions filled, 15% cancelled, and 10%
  remaining open. Postings are weighted toward high-volume departments: Sales (25%),
  Engineering (20%), Customer Support (12%), and Operations (10%).

  The dataset captures complete hiring lifecycle from posting to closure, including
  application volume metrics and departmental hiring patterns. Store-based roles (Sales,
  Customer Support, Operations, Facilities) include store assignments, while corporate
  roles (Engineering, Marketing, Finance, HR, Legal, Product, Data & Analytics) are
  HQ-based with null store_key values.

  Job titles are realistic and department-specific: Sales roles include "Store Manager"
  and "Senior Sales Associate", Engineering spans "Software Engineer" to "DevOps Engineer",
  and specialized departments have focused title sets. Posting durations typically range
  14-90 days for filled positions and 7-60 days for cancelled postings, reflecting
  realistic recruitment timelines.

  Generated deterministically using seed=42 for reproducible testing and cross-table
  joins. Supports talent acquisition analytics, departmental hiring velocity analysis,
  time-to-fill metrics, and organizational capacity planning. Links to departments
  and stores tables for comprehensive workforce analytics.
connection: bruin-playground-eu
tags:
  - domain:hr
  - domain:talent_acquisition
  - data_type:fact_table
  - data_type:transactional_data
  - sensitivity:internal
  - pipeline_role:raw
  - update_pattern:append_only
  - synthetic_data

materialization:
  type: table
  strategy: create+replace
image: python:3.11


columns:
  - name: posting_key
    type: INTEGER
    description: |
      Primary identifier for job postings, sequential integers 1-800 representing
      unique hiring requisitions. Each posting corresponds to a single role opening
      with distinct requirements and outcomes. Used as primary key and referenced
      in downstream hiring analytics and talent pipeline reporting.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: department_key
    type: INTEGER
    description: |
      Foreign key reference to departments table (2-12), indicating the hiring
      department for each posting. All postings assigned to operational departments
      (Executive excluded). Distribution reflects business priorities: Sales (25%),
      Engineering (20%), Customer Support (12%), Operations (10%), Marketing (8%),
      Finance (5%), HR (5%), Product (5%), Legal (2%), Data & Analytics (5%),
      Facilities (3%). Never null as all positions belong to specific departments.
    checks:
      - name: not_null
  - name: store_key
    type: FLOAT64
    description: |
      Foreign key reference to stores table for retail-based positions, null for
      corporate headquarters roles. Only populated for customer-facing departments:
      Sales, Customer Support, Operations, and Facilities (51.5% of all postings).
      When present, values range from store IDs 10-999999, indicating specific
      retail locations where the role is based. Corporate departments (Engineering,
      Marketing, Finance, HR, Legal, Product, Data & Analytics) always have null
      store_key as they operate from headquarters.
  - name: job_title
    type: VARCHAR
    description: |
      Human-readable job title for the posted position, department-specific and
      reflecting realistic career progressions. 31 distinct titles ranging 9-25
      characters. Sales titles include "Store Manager" and "Senior Sales Associate",
      Engineering spans "Software Engineer" to "DevOps Engineer", Marketing covers
      "Campaign Manager" to "Brand Manager". Titles indicate seniority levels and
      specialization within departments, supporting compensation analysis and
      organizational structure mapping.
    checks:
      - name: not_null
  - name: posted_date
    type: DATE
    description: |
      Date when the job posting was published and opened for applications. Spans
      2016-01-03 to 2026-10-27 with 720 distinct dates across 800 postings.
      Generated within the Contoso operational date range minus 60-day buffer
      to allow for realistic posting lifecycles. Always precedes closed_date
      when positions are filled or cancelled. Used for time-to-fill calculations
      and seasonal hiring pattern analysis.
    checks:
      - name: not_null
  - name: closed_date
    type: DATE
    description: |
      Date when the job posting was closed, either due to successful hire (filled)
      or business decision (cancelled). Null for 75 open positions (9.4%).
      When present, ranges 2016-01-17 to 2027-01-07 with 656 distinct dates.
      Filled positions closed 14-90 days after posting, cancelled positions
      closed 7-60 days after posting. Always follows posted_date chronologically.
      Used for recruitment cycle analysis and position lifecycle metrics.
  - name: status
    type: VARCHAR
    description: |
      Current posting status indicating hiring outcome. Three possible values:
      "Filled" (75% - successful hire), "Cancelled" (15% - business decision
      to not fill), and "Open" (10% - actively recruiting). String length
      4-9 characters. Status determines whether closed_date is populated
      and reflects realistic hiring success rates for established retail
      operations. Used for recruitment funnel analysis and hiring effectiveness.
    checks:
      - name: not_null
  - name: applicants
    type: INTEGER
    description: |
      Total number of applications received for each posting during its active
      period. Ranges 5-200 applicants with average of 102 per posting,
      reflecting realistic application volumes for retail and corporate roles.
      Never null as all postings generate applicant interest. Higher volume
      roles (Sales, Customer Support) typically attract more applicants than
      specialized positions (Legal, Data & Analytics). Used for recruitment
      efficiency metrics, sourcing effectiveness, and market demand analysis.
    checks:
      - name: not_null
  - name: extracted_at
    type: TIMESTAMP
    description: |
      ETL metadata timestamp indicating when this synthetic dataset was loaded
      into the warehouse (UTC). All 800 records share identical extraction time
      (2026-03-30 11:30:20.887291 UTC) since this is a static synthetic dataset
      generated deterministically. Used for data lineage tracking and pipeline
      audit purposes. In production systems, this would vary by ingestion batch.
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

NUM_POSTINGS = 800

DEPT_TITLES = {
    2: ["Sales Associate", "Senior Sales Associate", "Store Manager"],
    3: ["Marketing Coordinator", "Campaign Manager", "Brand Manager"],
    4: ["Accountant", "Financial Analyst", "Finance Manager"],
    5: ["HR Coordinator", "Recruiter", "HR Manager"],
    6: ["Software Engineer", "Senior Software Engineer", "DevOps Engineer", "QA Engineer"],
    7: ["Support Agent", "Senior Support Agent", "Support Team Lead"],
    8: ["Warehouse Associate", "Logistics Coordinator", "Operations Manager"],
    9: ["Paralegal", "Compliance Analyst"],
    10: ["Product Manager", "Associate Product Manager"],
    11: ["Maintenance Technician", "Facilities Coordinator"],
    12: ["Data Analyst", "Data Engineer", "Senior Data Analyst"],
}


def materialize():
    seed_all(42)
    rng = np.random.default_rng(42)
    keys = load_contoso_keys()
    store_keys = keys["store_keys"]
    min_date, max_date = keys["date_range"]

    logger.info("Generating %d job postings...", NUM_POSTINGS)

    dept_keys = list(DEPT_TITLES.keys())
    dept_weights = [0.25, 0.08, 0.05, 0.05, 0.20, 0.12, 0.10, 0.02, 0.05, 0.03, 0.05]
    dept_weights = [w / sum(dept_weights) for w in dept_weights]

    records = []
    for i in range(NUM_POSTINGS):
        dept_key = int(rng.choice(dept_keys, p=dept_weights))
        titles = DEPT_TITLES[dept_key]
        title = rng.choice(titles)

        days_range = (max_date - min_date).days - 60
        offset = int(rng.integers(0, max(1, days_range)))
        posted_date = (min_date + timedelta(days=offset)).date()

        # 75% filled, 15% cancelled, 10% open
        roll = rng.random()
        if roll < 0.75:
            status = "Filled"
            closed_date = posted_date + timedelta(days=int(rng.integers(14, 90)))
        elif roll < 0.90:
            status = "Cancelled"
            closed_date = posted_date + timedelta(days=int(rng.integers(7, 60)))
        else:
            status = "Open"
            closed_date = None

        applicants = int(rng.integers(5, 200))

        store_key = None
        if dept_key in (2, 7, 8, 11):
            store_key = int(rng.choice(store_keys))

        records.append({
            "PostingKey": i + 1,
            "DepartmentKey": dept_key,
            "StoreKey": store_key,
            "JobTitle": title,
            "PostedDate": posted_date,
            "ClosedDate": closed_date,
            "Status": status,
            "Applicants": applicants,
            "HiredEmployeeKey": None,  # simplified: not linked to specific employees
        })

    df = pd.DataFrame(records)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d job postings", len(df))
    return df
