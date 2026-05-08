"""@bruin

name: contoso_v2_raw.job_postings
description: |
  Job postings for Contoso v2, 2010-01-01 to 2026-05-01. ~1,800 postings
  clustered around real hiring seasons: July-August (back-to-school, ~35%) and
  Oct-Nov (Q4 holiday, ~30%); near-zero March-August 2020 (COVID hiring
  freeze); 2.5x cadence 2023-2024 (AI boom). 70% of Filled postings link to a
  real employee via hired_employee_key. Applicant counts gamma-distributed,
  with senior roles drawing 5x more applicants.
connection: gcp-default
tags:
  - hr
  - recruiting

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: posting_key
    type: INTEGER
    primary_key: true
    description: Sparse posting identifier.
    checks:
      - name: not_null
      - name: unique
  - name: department_key
    type: INTEGER
    description: Department the posting is for.
    checks:
      - name: not_null
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
  - name: posted_date
    type: DATE
    description: Date the role was posted.
    checks:
      - name: not_null
  - name: closed_date
    type: DATE
    description: Date the role closed.
  - name: status
    type: VARCHAR
    description: Filled, Cancelled, or Open.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Filled
          - Cancelled
          - Open
  - name: applicants
    type: INTEGER
    description: Number of applicants received.
    checks:
      - name: not_null
      - name: non_negative
  - name: hired_employee_key
    type: INTEGER
    description: Employee hired for this posting (NULL if Cancelled/Open or unlinked).
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
    PIPELINE_START, PIPELINE_END, build_employees, get_seeded_rng,
    regime_for, sparse_keys,
)

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)

# Per-year base postings, modulated by regime (AI boom doubles)
def _yearly_postings(year: int) -> int:
    base = {
        2010: 30, 2011: 50, 2012: 65, 2013: 80, 2014: 100, 2015: 130,
        2016: 160, 2017: 175, 2018: 175, 2019: 165, 2020: 90,
        2021: 130, 2022: 145, 2023: 280, 2024: 320, 2025: 175, 2026: 70,
    }
    return base.get(year, 100)


# Department weights — higher Q4 for retail, higher year-round for tech
DEPT_WEIGHTS = {
    1: 0.01, 2: 0.30, 3: 0.07, 4: 0.05, 5: 0.05, 6: 0.20, 7: 0.10,
    8: 0.07, 9: 0.02, 10: 0.05, 11: 0.02, 12: 0.06,
}

DEPT_TITLES = {
    1: ["VP", "Director"],
    2: ["Sales Associate", "Sales Specialist", "Account Executive", "Sales Manager", "Regional Director"],
    3: ["Marketing Coordinator", "Campaign Manager", "Brand Manager"],
    4: ["Financial Analyst", "Accountant", "Controller"],
    5: ["Recruiter", "HR Specialist", "HR Manager"],
    6: ["Software Engineer", "Senior Engineer", "Staff Engineer", "Engineering Manager"],
    7: ["Support Agent", "Senior Support Agent", "Support Manager"],
    8: ["Operations Coordinator", "Logistics Lead", "Operations Manager"],
    9: ["Counsel", "General Counsel"],
    10: ["Product Analyst", "Product Manager"],
    11: ["Facilities Tech", "Facilities Manager"],
    12: ["Data Analyst", "Data Engineer", "Analytics Manager"],
}


def materialize():
    rng = get_seeded_rng(83)
    employees = build_employees()
    emp_by_dept_year = {}
    for _, e in employees.iterrows():
        hd = pd.to_datetime(e["hire_date"]).date()
        emp_by_dept_year.setdefault((int(e["department_key"]), hd.year), []).append(int(e["employee_key"]))

    sparse_iter = iter(sparse_keys(50_000, gap_rate=0.18, start=10001, seed=83))

    rows = []
    for year in range(2010, 2027):
        n_postings = _yearly_postings(year)
        # Month weights
        month_weights = np.array([0.05, 0.05, 0.07, 0.07, 0.07, 0.06, 0.13, 0.13, 0.10, 0.13, 0.10, 0.04])
        if year == 2020:
            # COVID freeze March-August
            month_weights = np.array([0.10, 0.10, 0.02, 0.01, 0.01, 0.01, 0.02, 0.03, 0.10, 0.20, 0.20, 0.20])
        month_weights = month_weights / month_weights.sum()

        # Department weights — boost engineering during AI boom
        dept_weights = dict(DEPT_WEIGHTS)
        if year >= 2023:
            dept_weights[6] = 0.32
            dept_weights[12] = 0.10
            dept_weights[10] = 0.08
        if year < 2015:
            for k in (6, 10, 12):
                dept_weights[k] = 0.0
        dk = list(dept_weights.keys())
        dw = np.array([dept_weights[k] for k in dk])
        dw = dw / dw.sum()

        for _ in range(n_postings):
            posting_key = next(sparse_iter)
            month = int(rng.choice(range(1, 13), p=month_weights))
            day = int(rng.integers(1, 28))
            try:
                posted = date(year, month, day)
            except ValueError:
                posted = date(year, month, 15)
            if posted > PIPELINE_END:
                continue

            dept = int(rng.choice(dk, p=dw))
            level_choices = ["L1", "L1", "L2", "L2", "L3", "L4", "L5"]
            level = str(rng.choice(level_choices))
            title = str(rng.choice(DEPT_TITLES[dept]))

            # Status
            r = rng.random()
            if r < 0.78:
                status = "Filled"
                duration = int(rng.integers(14, 90))
                closed = posted + timedelta(days=duration)
            elif r < 0.92:
                status = "Cancelled"
                duration = int(rng.integers(7, 60))
                closed = posted + timedelta(days=duration)
            else:
                status = "Open"
                closed = None

            # Applicants — gamma-distributed, more for senior
            level_mult = {"L1": 1.0, "L2": 1.5, "L3": 2.5, "L4": 4.0, "L5": 5.5}.get(level, 1.0)
            n_apps = int(rng.gamma(shape=2.5, scale=20) * level_mult)
            if rng.random() < 0.05:
                # 5% high-profile postings get spike
                n_apps *= int(rng.integers(3, 7))

            hired_emp = None
            if status == "Filled" and rng.random() < 0.7:
                # Pick from employees hired in same dept around closed date
                pool = emp_by_dept_year.get((dept, closed.year if closed else year), [])
                if pool:
                    hired_emp = int(rng.choice(pool))

            rows.append({
                "posting_key": int(posting_key),
                "department_key": dept,
                "job_title": title,
                "level": level,
                "posted_date": posted,
                "closed_date": closed,
                "status": status,
                "applicants": n_apps,
                "hired_employee_key": hired_emp,
            })

    df = pd.DataFrame(rows)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d job postings", len(df))
    return df
