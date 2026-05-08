"""@bruin

name: contoso_v2_raw.accounts_payable
description: |
  Accounts payable invoices for Contoso v2, 2010-01-01 to 2026-05-01.
  ~30K invoices across 200+ named vendors. Vendor concentration follows a
  Pareto distribution (top 5 vendors take ~20% of total spend). Payment
  status is correlated to amount: invoices >$50K are 5x more likely to be
  Pending. Aging spikes during 2020-04 to 2020-07 (15% Overdue rate vs
  ~5% baseline) and 2022-Q3 (12%) reflecting cash-flow stress. Invoice
  amounts inflate ~8% during 2022 inflation regime. ~1.5% duplicate-invoice
  errors (same invoice_number under different ap_keys).
connection: gcp-default
tags:
  - finance
  - accounts_payable
  - fact_table

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: ap_key
    type: INTEGER
    primary_key: true
    description: Sparse AP record identifier.
    checks:
      - name: not_null
      - name: unique
  - name: vendor_name
    type: VARCHAR
    description: Vendor company name.
    checks:
      - name: not_null
  - name: invoice_number
    type: VARCHAR
    description: Vendor invoice number (1.5% duplicates due to data entry errors).
    checks:
      - name: not_null
  - name: invoice_date
    type: DATE
    description: Invoice date.
    checks:
      - name: not_null
  - name: due_date
    type: DATE
    description: Payment due date.
    checks:
      - name: not_null
  - name: paid_date
    type: DATE
    description: Date paid (NULL if Pending or Overdue).
  - name: amount
    type: NUMERIC
    description: Invoice amount in USD.
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
    description: Paid, Pending, or Overdue.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Paid
          - Pending
          - Overdue
  - name: department_key
    type: INTEGER
    description: Foreign key to departments.
    checks:
      - name: not_null
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
    PIPELINE_END, PIPELINE_START, event_impact, get_seeded_faker,
    get_seeded_rng, regime_for, sparse_keys,
)

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)


VENDOR_PREFIXES = [
    "Acme", "Apex", "Atlas", "Beacon", "Bluepeak", "Bridgepoint", "Cardinal",
    "Cedar", "Citadel", "Clearwater", "Cobalt", "Compass", "Concourse",
    "Corebridge", "Creststone", "Crosswind", "Daybreak", "Edgewater",
    "Elevate", "Emerald", "Fairmont", "Fieldstone", "Fortress", "Foundry",
    "Granite", "Greenline", "Halo", "Harbor", "Heritage", "Highland",
    "Horizon", "Indigo", "Ironclad", "Keystone", "Lakeside", "Lighthouse",
    "Linden", "Magnolia", "Maple", "Meridian", "Monarch", "Northbeam",
    "Oakwood", "Onyx", "Pacific", "Pinnacle", "Polaris", "Prairie", "Quantum",
    "Redwood", "Sage", "Sentinel", "Silverline", "Spruce", "Stonebridge",
    "Sterling", "Summit", "Sundial", "Sycamore", "Tempest", "Thornton",
    "Tradewind", "Triton", "Vanguard", "Veritas", "Vista", "Westbrook",
    "Westfield", "Whitestone", "Wildwood", "Willow", "Windsor",
]
VENDOR_SUFFIXES = [
    "Logistics", "Tech", "Solutions", "Partners", "Capital", "Industries",
    "Systems", "Services", "Group", "Consulting", "Networks", "Holdings",
    "Software", "Logistics", "Distribution", "Equipment", "Supply Co",
    "Media", "Insurance", "Realty", "Print", "Office Co", "Cloud",
    "Hardware", "Logistics Inc", "Manufacturing", "Marketing",
]


def _build_vendors(rng: np.random.Generator, n: int = 220) -> list[str]:
    seen = set()
    out = []
    while len(out) < n:
        name = (f"{VENDOR_PREFIXES[int(rng.integers(0, len(VENDOR_PREFIXES)))]}"
                f" {VENDOR_SUFFIXES[int(rng.integers(0, len(VENDOR_SUFFIXES)))]}")
        if name in seen:
            continue
        seen.add(name)
        out.append(name)
    return out


def _yearly_invoice_count(year: int) -> int:
    base = {
        2010: 800, 2011: 950, 2012: 1100, 2013: 1300, 2014: 1500,
        2015: 1700, 2016: 1900, 2017: 2100, 2018: 2200, 2019: 2300,
        2020: 1900, 2021: 2100, 2022: 2400, 2023: 3000, 2024: 3300,
        2025: 2700, 2026: 1100,
    }
    return base.get(year, 1500)


def materialize():
    rng = get_seeded_rng(191)
    fake = get_seeded_faker(191)
    vendors = _build_vendors(rng)

    # Pareto-style spend concentration: top 5 vendors get heavy weight
    n_v = len(vendors)
    weights = np.array([1.0 / (i + 1) ** 0.7 for i in range(n_v)])
    weights = weights / weights.sum()

    sparse_iter = iter(sparse_keys(80_000, gap_rate=0.10, start=1, seed=191))

    rows = []
    for year in range(2010, 2027):
        n = _yearly_invoice_count(year)
        for _ in range(n):
            month = int(rng.integers(1, 13))
            day = int(rng.integers(1, 28))
            try:
                invoice_date = date(year, month, day)
            except ValueError:
                invoice_date = date(year, month, 15)
            if invoice_date < PIPELINE_START or invoice_date > PIPELINE_END:
                continue

            vendor = vendors[int(rng.choice(n_v, p=weights))]
            dept_key = int(rng.integers(1, 13))

            payment_terms = int(rng.choice([15, 30, 45, 60], p=[0.15, 0.55, 0.20, 0.10]))
            due_date = invoice_date + timedelta(days=payment_terms)

            # Lognormal amount, inflate during inflation regime
            amount = float(rng.lognormal(mean=8.0, sigma=1.2))
            amount *= event_impact(invoice_date, "cost")
            amount = min(amount, 500_000)
            amount = round(amount, 2)

            # Status — large invoices more likely Pending; aging spikes in COVID
            regime = regime_for(invoice_date)
            base_paid_p = 0.85
            if regime == "covid_shock" or (regime == "recovery"
                                           and invoice_date <= date(2020, 7, 31)):
                paid_p = 0.70
                overdue_p = 0.15
            elif regime == "inflation" and invoice_date.month in (7, 8, 9):
                paid_p = 0.78
                overdue_p = 0.12
            else:
                paid_p = base_paid_p
                overdue_p = 0.05

            # Large invoices skew toward Pending
            if amount > 50_000:
                paid_p *= 0.65
                pending_lift = 0.20
            else:
                pending_lift = 0.0

            r = rng.random()
            if r < paid_p:
                status = "Paid"
                days_to_pay = int(rng.integers(1, payment_terms + 15))
                paid_date = invoice_date + timedelta(days=days_to_pay)
                if paid_date > PIPELINE_END:
                    paid_date = None
                    status = "Pending"
            elif r < paid_p + (1 - paid_p - overdue_p) + pending_lift:
                status = "Pending"
                paid_date = None
            else:
                status = "Overdue"
                paid_date = None

            invoice_num = f"INV-{fake.bothify('####-????').upper()}"

            rows.append({
                "ap_key": int(next(sparse_iter)),
                "vendor_name": vendor,
                "invoice_number": invoice_num,
                "invoice_date": invoice_date,
                "due_date": due_date,
                "paid_date": paid_date,
                "amount": amount,
                "currency": "USD",
                "status": status,
                "department_key": dept_key,
            })

    df = pd.DataFrame(rows)
    # 1.5% duplicate-invoice errors: pick rows and clone invoice_number with new ap_key
    n_dup = max(1, int(len(df) * 0.015))
    dup_idx = rng.choice(len(df), size=n_dup, replace=False)
    dups = df.iloc[dup_idx].copy()
    dups["ap_key"] = [int(next(sparse_iter)) for _ in range(n_dup)]
    dups["amount"] = (dups["amount"] * np.array(
        [float(rng.uniform(0.99, 1.01)) for _ in range(n_dup)]
    )).round(2)
    df = pd.concat([df, dups], ignore_index=True)

    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d AP invoices (%d duplicates)", len(df), n_dup)
    return df
