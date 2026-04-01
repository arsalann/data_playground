"""@bruin

name: contoso_raw.dates
description: |
  Comprehensive date dimension table for the Contoso consumer electronics retailer simulation.
  Provides calendar attributes for time-based analysis across all business departments (Sales, HR, Finance, Marketing, Engineering, Operations, Support).

  Spans 11 years (2016-01-01 to 2026-12-31) with 4,018 daily records containing hierarchical date breakdowns,
  working day classifications, and various formatting options for reporting flexibility.

  Source: SQLBI Contoso Data Generator V2 (MIT license) - industry-standard reference dataset for retail analytics.
  Used as the primary time dimension for joins with fact tables across the entire Contoso data warehouse.
connection: bruin-playground-eu
tags:
  - dimension_table
  - reference_data
  - time_dimension
  - retail_simulation
  - sqlbi_contoso

materialization:
  type: table
  strategy: create+replace
image: python:3.11


columns:
  - name: date
    type: TIMESTAMP
    description: Primary calendar date (midnight UTC timestamp). Natural key for date-based joins.
    primary_key: true
    checks:
      - name: not_null
  - name: date_key
    type: VARCHAR
    description: Date in YYYYMMDD string format (e.g., "20220315"). Alternative key optimized for string-based lookups and partitioning.
    checks:
      - name: not_null
      - name: unique
  - name: year
    type: INTEGER
    description: Calendar year (2016-2026). Useful for year-over-year analysis and annual aggregations.
    checks:
      - name: not_null
  - name: year_quarter
    type: VARCHAR
    description: Year and quarter in readable format (e.g., "2022-Q3"). Human-friendly quarterly period identifier.
    checks:
      - name: not_null
  - name: year_quarter_number
    type: INTEGER
    description: Year-quarter as integer for sorting/comparison (e.g., 20223 for 2022-Q3). Enables chronological ordering of quarters.
    checks:
      - name: not_null
  - name: quarter
    type: VARCHAR
    description: Quarter designation only ("Q1", "Q2", "Q3", "Q4"). Useful for seasonal analysis patterns.
    checks:
      - name: not_null
  - name: year_month
    type: VARCHAR
    description: Year and full month name (e.g., "2022-March"). Long-form monthly period identifier for detailed reporting.
    checks:
      - name: not_null
  - name: year_month_short
    type: VARCHAR
    description: Year and abbreviated month (e.g., "2022-Mar"). Compact monthly identifier for dashboards with space constraints.
    checks:
      - name: not_null
  - name: year_month_number
    type: INTEGER
    description: Year-month as integer (e.g., 202203). Numeric format enables efficient monthly sorting and filtering.
    checks:
      - name: not_null
  - name: month
    type: VARCHAR
    description: Full month name ("January" through "December"). Useful for month-based grouping and seasonal trend analysis.
    checks:
      - name: not_null
  - name: month_short
    type: VARCHAR
    description: Three-letter month abbreviation ("Jan" through "Dec"). Space-efficient month representation for charts and reports.
    checks:
      - name: not_null
  - name: month_number
    type: INTEGER
    description: Numeric month (1-12, where 1=January). Enables month-based mathematical operations and sorting.
    checks:
      - name: not_null
  - name: dayof_week
    type: VARCHAR
    description: Full day name ("Monday" through "Sunday"). Essential for day-of-week analysis and weekly pattern identification.
    checks:
      - name: not_null
  - name: dayof_week_short
    type: VARCHAR
    description: Three-letter day abbreviation ("Mon" through "Sun"). Compact day representation for weekly charts and grids.
    checks:
      - name: not_null
  - name: dayof_week_number
    type: INTEGER
    description: Day of week as integer (1=Monday, 7=Sunday, ISO 8601 standard). Enables day-based sorting and weekly calculations.
    checks:
      - name: not_null
  - name: working_day
    type: INTEGER
    description: Binary working day flag (1=business day, 0=weekend). Approximately 69% of dates are working days. Critical for business day calculations.
    checks:
      - name: not_null
  - name: working_day_number
    type: INTEGER
    description: Sequential counter of working days since start of dataset. Enables business day arithmetic and working day trend analysis.
    checks:
      - name: not_null
  - name: extracted_at
    type: TIMESTAMP
    description: ETL processing timestamp (UTC). Records when this data was loaded into the warehouse for lineage tracking.

@bruin"""

import logging
import os
from datetime import datetime, timezone

import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _contoso_helpers import load_parquet

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def materialize():
    logger.info("Loading Contoso V2 dates data...")
    df = load_parquet("dates")
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Fetched %d rows", len(df))
    return df
