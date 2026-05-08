"""@bruin

name: contoso_v2_raw.support_tickets
description: |
  Customer support tickets for Contoso v2, 2010-01-01 to 2026-05-01.
  ~250K tickets total over 16 years. Volume scales with sales (5x lift in
  Q4 holiday window) and 2x during 2020-03 to 2020-06 COVID surge
  (concentrated in Shipping and Billing categories). 2023 spike of
  "Account Locked" tickets reflects a post-breach narrative. Category-
  correlated CSAT: Shipping ~2.1/5, Product Quality ~3.2/5, Account
  ~4.4/5. Resolution time gamma-distributed and correlated to priority +
  channel: Phone ~4h median, Email ~28h median.
connection: gcp-default
instance: b1.medium
tags:
  - support
  - fact_table

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: support_ticket_key
    type: INTEGER
    primary_key: true
    description: Sparse ticket identifier.
    checks:
      - name: not_null
      - name: unique
  - name: customer_key
    type: INTEGER
    description: Foreign key to customers.
    checks:
      - name: not_null
  - name: order_key
    type: INTEGER
    description: Foreign key to orders (~80% populated).
  - name: product_key
    type: INTEGER
    description: Foreign key to products (~70% populated).
  - name: channel
    type: VARCHAR
    description: Phone, Email, Chat, or Social.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Phone
          - Email
          - Chat
          - Social
  - name: category
    type: VARCHAR
    description: Issue category.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Shipping
          - Returns
          - Product Quality
          - Billing
          - Account
          - Technical
  - name: priority
    type: VARCHAR
    description: Critical, High, Medium, Low.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Critical
          - High
          - Medium
          - Low
  - name: status
    type: VARCHAR
    description: Resolved, Open, or Escalated.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Resolved
          - Open
          - Escalated
  - name: created_date
    type: DATE
    description: Date the ticket was created.
    checks:
      - name: not_null
  - name: resolved_date
    type: DATE
    description: Date the ticket was resolved (NULL for Open/Escalated).
  - name: resolution_hours
    type: NUMERIC
    description: Hours from created to resolved (NULL for Open/Escalated).
  - name: satisfaction_score
    type: INTEGER
    description: 1-5 CSAT score (~70% response rate among resolved).
  - name: agent_employee_key
    type: INTEGER
    description: Foreign key to employees (Customer Support, dept_key=7).
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
    PIPELINE_END, build_customers, build_employees, build_orders,
    build_products, get_seeded_rng, regime_for, seasonal_multiplier,
    sparse_keys,
)

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)

SUPPORT_DEPT_KEY = 7

CHANNELS = ["Phone", "Email", "Chat", "Social"]
CATEGORIES = ["Shipping", "Returns", "Product Quality", "Billing", "Account", "Technical"]
PRIORITIES = ["Critical", "High", "Medium", "Low"]

CSAT_BY_CATEGORY = {
    "Shipping":         np.array([0.30, 0.35, 0.20, 0.10, 0.05]),
    "Returns":          np.array([0.20, 0.25, 0.25, 0.20, 0.10]),
    "Product Quality":  np.array([0.10, 0.20, 0.30, 0.25, 0.15]),
    "Billing":          np.array([0.15, 0.25, 0.25, 0.20, 0.15]),
    "Account":          np.array([0.03, 0.05, 0.12, 0.30, 0.50]),
    "Technical":        np.array([0.10, 0.15, 0.25, 0.30, 0.20]),
}

CHANNEL_RESOLUTION_HOURS = {
    "Phone":  4.0,
    "Chat":   8.0,
    "Email":  28.0,
    "Social": 18.0,
}


def _daily_volume(d: date) -> int:
    base = 30  # tickets/day baseline
    s = seasonal_multiplier(d) / max(1.0, 1.0)  # already includes Q4 lift
    n = base * s
    regime = regime_for(d)
    if regime == "covid_shock":
        n *= 2.0
    return max(1, int(n))


def _category_weights(d: date) -> np.ndarray:
    w = np.array([0.20, 0.15, 0.20, 0.15, 0.15, 0.15])
    regime = regime_for(d)
    if regime == "covid_shock":
        # Surge in Shipping (idx 0) and Billing (idx 3)
        w = np.array([0.40, 0.10, 0.10, 0.25, 0.05, 0.10])
    elif d.year == 2023 and d.month <= 3:
        # Account-Locked surge after breach
        w = np.array([0.18, 0.12, 0.18, 0.12, 0.30, 0.10])
    return w / w.sum()


def materialize():
    rng = get_seeded_rng(211)
    customers = build_customers()
    products = build_products()
    orders = build_orders()
    employees = build_employees()
    support = employees[employees["department_key"] == SUPPORT_DEPT_KEY].copy()
    if support.empty:
        logger.warning("No support employees; skipping support_tickets")
        return pd.DataFrame()

    cust_keys = customers["customer_key"].values
    cust_signup = pd.to_datetime(customers["signup_date"]).dt.date.values
    prod_keys = products["product_key"].values
    order_keys = orders["order_key"].values
    order_dates = pd.to_datetime(orders["order_date"]).dt.date.values

    sup_keys = support["employee_key"].tolist()
    sup_hire = pd.to_datetime(support["hire_date"]).dt.date.tolist()
    sup_term = [
        d.date() if pd.notna(d) else None
        for d in pd.to_datetime(support["termination_date"])
    ]

    sparse_iter = iter(sparse_keys(500_000, gap_rate=0.06, start=1, seed=211))

    rows = []
    cur = date(2010, 1, 1)
    while cur <= PIPELINE_END:
        n = _daily_volume(cur)
        cat_weights = _category_weights(cur)

        eligible_agents = [
            i for i, hd in enumerate(sup_hire)
            if hd <= cur and (sup_term[i] is None or sup_term[i] >= cur)
        ]
        if not eligible_agents:
            cur += timedelta(days=1)
            continue

        # Eligible customers (signed up by cur)
        cust_mask = cust_signup <= cur
        cust_idx_eligible = np.flatnonzero(cust_mask)
        if len(cust_idx_eligible) == 0:
            cur += timedelta(days=1)
            continue
        # Eligible orders (placed by cur)
        order_mask = order_dates <= cur
        order_idx_eligible = np.flatnonzero(order_mask)

        for _ in range(n):
            cust_i = int(rng.choice(cust_idx_eligible))
            channel = str(rng.choice(CHANNELS, p=[0.20, 0.40, 0.30, 0.10]))
            category = str(rng.choice(CATEGORIES, p=cat_weights))
            priority = str(rng.choice(PRIORITIES, p=[0.05, 0.15, 0.55, 0.25]))

            order_key = None
            if rng.random() < 0.80 and len(order_idx_eligible) > 0:
                order_key = int(order_keys[int(rng.choice(order_idx_eligible))])

            product_key = None
            if rng.random() < 0.70:
                product_key = int(rng.choice(prod_keys))

            # Status mix
            r = rng.random()
            if r < 0.85:
                status = "Resolved"
            elif r < 0.95:
                status = "Open"
            else:
                status = "Escalated"

            # Resolution time — gamma scaled by priority + channel base
            base_hours = CHANNEL_RESOLUTION_HOURS[channel]
            prio_mult = {"Critical": 0.4, "High": 0.7, "Medium": 1.0, "Low": 1.5}[priority]
            mean_hours = base_hours * prio_mult
            res_hours = None
            resolved_date = None
            if status == "Resolved":
                res_hours = float(rng.gamma(shape=2.0, scale=mean_hours / 2.0))
                resolved_dt = datetime(cur.year, cur.month, cur.day) + timedelta(hours=res_hours)
                if resolved_dt.date() <= PIPELINE_END:
                    resolved_date = resolved_dt.date()
                else:
                    status = "Open"
                    res_hours = None

            # CSAT
            sat = None
            if status == "Resolved" and rng.random() < 0.70:
                sat_w = CSAT_BY_CATEGORY[category]
                sat = int(rng.choice([1, 2, 3, 4, 5], p=sat_w))

            agent = int(sup_keys[int(rng.choice(eligible_agents))])

            rows.append({
                "support_ticket_key": int(next(sparse_iter)),
                "customer_key": int(cust_keys[cust_i]),
                "order_key": order_key,
                "product_key": product_key,
                "channel": channel,
                "category": category,
                "priority": priority,
                "status": status,
                "created_date": cur,
                "resolved_date": resolved_date,
                "resolution_hours": round(res_hours, 2) if res_hours is not None else None,
                "satisfaction_score": sat,
                "agent_employee_key": agent,
            })
        cur += timedelta(days=1)

    df = pd.DataFrame(rows)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d support tickets", len(df))
    return df
