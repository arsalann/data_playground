"""@bruin

name: contoso_raw.accounts_payable
description: |
  Contoso accounts payable invoices (synthetic).

  Simulates a realistic AP system for a consumer electronics retailer with 15 vendor types
  across 12 departments. Contains 12,000 deterministically generated invoices spanning 2016-2026
  with realistic payment patterns: 85% paid, 10% pending, 5% overdue. Invoice amounts follow
  a lognormal distribution (median ~$3K, capped at $500K) with payment terms of 15/30/45/60 days.

  Key business characteristics:
  - Vendor invoices follow INV-####-???? numbering pattern
  - Payment terms vary by vendor type (15, 30, 45, or 60 days)
  - Department coverage spans all business units (Sales, HR, Finance, etc.)
  - All amounts standardized in USD
  - Deterministic via seed=42 for reproducible testing
connection: contoso-duckdb
tags:
  - domain:finance
  - data_type:fact_table
  - pipeline_role:raw
  - update_pattern:create_replace
  - sensitivity:internal
  - source:synthetic

materialization:
  type: table
  strategy: create+replace
image: python:3.11


columns:
  - name: ap_key
    type: INTEGER
    description: Unique AP invoice identifier - sequential primary key starting from 1
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: vendor_name
    type: VARCHAR
    description: Vendor company name - 15 predefined vendor types including tech, logistics, and service providers
    checks:
      - name: not_null
  - name: invoice_number
    type: VARCHAR
    description: Vendor invoice number - unique 13-character identifier following pattern INV-####-????
    checks:
      - name: not_null
      - name: unique
  - name: invoice_date
    type: DATE
    description: Invoice date - randomly distributed across date range with 60-day buffer from max date
    checks:
      - name: not_null
  - name: due_date
    type: DATE
    description: Payment due date - calculated as invoice_date + payment_terms (15/30/45/60 days)
    checks:
      - name: not_null
  - name: paid_date
    type: DATE
    description: Date payment was made - null for unpaid invoices (15% of records), otherwise 1 to payment_terms+15 days after invoice
  - name: amount
    type: DOUBLE
    description: Invoice amount in USD - lognormal distribution with mean=8, sigma=1.2, capped at $500,000
    checks:
      - name: not_null
      - name: non_negative
  - name: currency
    type: VARCHAR
    description: Currency code - standardized to 'USD' for all records
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - USD
  - name: status
    type: VARCHAR
    description: Invoice status - 85% 'Paid', 10% 'Pending', 5% 'Overdue' based on payment and due dates
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Paid
          - Pending
          - Overdue
  - name: department_key
    type: INTEGER
    description: Foreign key to departments table - references 12 business unit departments (1-12)
    checks:
      - name: not_null
      - name: min
        value: 1
      - name: max
        value: 12
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when data was loaded (UTC) - consistent across full dataset execution
    checks:
      - name: not_null

@bruin"""

import logging
import os
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _contoso_helpers import get_seeded_faker, load_contoso_keys, seed_all

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

NUM_INVOICES = 12000

VENDOR_TYPES = [
    "Tech Solutions Inc.", "Global Logistics Corp.", "Office Supplies Direct",
    "CloudServe Partners", "DataLink Systems", "SecureTech LLC",
    "Creative Media Group", "Talent Recruiters Int.", "Legal Associates LLP",
    "Facilities Mgmt Co.", "Premium Shipping Ltd.", "Digital Marketing Pro",
    "Software License Corp.", "Hardware Distributors Inc.", "Consulting Group Intl.",
]


def materialize():
    seed_all(42)
    rng = np.random.default_rng(42)
    fake = get_seeded_faker(42)
    keys = load_contoso_keys()
    min_date, max_date = keys["date_range"]

    logger.info("Generating %d AP invoices...", NUM_INVOICES)

    dept_keys = list(range(1, 13))
    records = []

    for i in range(NUM_INVOICES):
        dept_key = int(rng.choice(dept_keys))
        vendor = rng.choice(VENDOR_TYPES)

        days_range = (max_date - min_date).days - 60
        offset = int(rng.integers(0, max(1, days_range)))
        invoice_date = (min_date + timedelta(days=offset)).date()

        payment_terms = int(rng.choice([15, 30, 45, 60]))
        due_date = invoice_date + timedelta(days=payment_terms)

        amount = round(rng.lognormal(mean=8, sigma=1.2), 2)
        amount = min(amount, 500_000)

        # 85% paid, 10% pending, 5% overdue
        roll = rng.random()
        if roll < 0.85:
            days_to_pay = int(rng.integers(1, payment_terms + 15))
            paid_date = invoice_date + timedelta(days=days_to_pay)
            status = "Paid"
        elif roll < 0.95:
            paid_date = None
            status = "Pending"
        else:
            paid_date = None
            status = "Overdue"

        invoice_num = f"INV-{fake.bothify('####-????').upper()}"

        records.append({
            "APKey": i + 1,
            "VendorName": vendor,
            "InvoiceNumber": invoice_num,
            "InvoiceDate": invoice_date,
            "DueDate": due_date,
            "PaidDate": paid_date,
            "Amount": amount,
            "Currency": "USD",
            "Status": status,
            "DepartmentKey": dept_key,
        })

    df = pd.DataFrame(records)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d AP invoices", len(df))
    return df
