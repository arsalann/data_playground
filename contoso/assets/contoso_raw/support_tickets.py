"""@bruin

name: contoso_raw.support_tickets
description: |
  Comprehensive customer support ticket dataset for Contoso's consumer electronics business.

  Contains 150,000 synthetic support interactions spanning 2016-2026, generated deterministically
  (seed=42) to simulate realistic customer service operations. Each ticket represents a customer
  inquiry or issue across multiple communication channels (Phone, Email, Chat, Social) and
  categories (Shipping, Returns, Product Quality, Billing, Account, Technical).

  Key business characteristics:
  - 80% of tickets reference a specific order (order_key populated)
  - 70% relate to a specific product (product_key populated)
  - 85% resolution rate with 1-72 hour typical resolution times
  - Customer satisfaction surveys collected for ~70% of resolved tickets (1-5 scale)
  - Support agents from employee department 7 (keys 1651-1950)

  Channel distribution reflects realistic usage patterns: Email (35%), Chat (30%), Phone (25%),
  Social (10%). Priority distribution: Medium (55%), Low (25%), High (15%), Critical (5%).

  This raw dataset feeds downstream analytics for operational KPIs, agent performance tracking,
  customer satisfaction monitoring, and resource planning. Contains typical support operation
  challenges like escalations (5%), open tickets requiring follow-up, and varying satisfaction
  scores across different issue types and resolution methods.
connection: bruin-playground-eu
tags:
  - customer_support
  - raw_data
  - synthetic_data
  - fact_table
  - deterministic_generation
  - consumer_electronics
  - operational_data
  - satisfaction_tracking

materialization:
  type: table
  strategy: create+replace
image: python:3.11

secrets:
  - key: bruin-playground-eu
    inject_as: bruin-playground-eu

columns:
  - name: SupportTicketKey
    type: INTEGER
    description: |
      Sequential unique identifier for each support ticket (1-150,000).
      Primary key for the table, used for deduplication in downstream processing.
      Generated deterministically for reproducible synthetic data.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: CustomerKey
    type: INTEGER
    description: |
      Foreign key reference to customers table. Every ticket is associated with a customer.
      Represents the customer who initiated the support request.
      No null values - all support interactions must have a customer context.
    checks:
      - name: not_null
  - name: OrderKey
    type: INTEGER
    description: |
      Foreign key reference to orders table. Populated for ~80% of tickets.
      Null for general account inquiries, technical support, or pre-purchase questions.
      When populated, indicates ticket relates to a specific transaction or delivery issue.
  - name: ProductKey
    type: INTEGER
    description: |
      Foreign key reference to products table. Populated for ~70% of tickets.
      Null for account management, billing, or general service inquiries.
      When populated, indicates product-specific issues like quality, compatibility, or usage questions.
  - name: Channel
    type: VARCHAR
    description: |
      Customer communication channel used to submit the support request.
      Values: Phone (25%), Email (35%), Chat (30%), Social (10%).
      Distribution reflects typical customer preferences and channel availability.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Phone
          - Email
          - Chat
          - Social
  - name: Category
    type: VARCHAR
    description: |
      Issue classification category assigned by support system or agent.
      Common categories: Shipping, Returns, Product Quality, Billing, Account, Technical.
      Used for routing tickets to specialized teams and performance analysis.
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
  - name: Priority
    type: VARCHAR
    description: |
      Business priority level assigned based on issue impact and urgency.
      Distribution: Critical (5%), High (15%), Medium (55%), Low (25%).
      Critical tickets typically involve service outages or revenue impact.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Critical
          - High
          - Medium
          - Low
  - name: Status
    type: VARCHAR
    description: |
      Current ticket resolution status. Distribution: Resolved (85%), Open (10%), Escalated (5%).
      Resolved indicates successful closure, Open requires follow-up, Escalated needs specialist attention.
      Used for SLA tracking and agent performance measurement.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Resolved
          - Open
          - Escalated
  - name: CreatedDate
    type: DATE
    description: |
      Date when the support ticket was initially created (customer inquiry received).
      Date range spans 2016-2026 for comprehensive historical and future analysis.
      Used for trend analysis, volume planning, and seasonal pattern identification.
    checks:
      - name: not_null
  - name: ResolvedDate
    type: DATE
    description: |
      Date when the ticket was marked as resolved. Null for Open and Escalated tickets (~15%).
      Used to calculate resolution time SLAs and agent efficiency metrics.
      Always >= CreatedDate when populated (typical range: 0-3 days).
  - name: SatisfactionScore
    type: INTEGER
    description: |
      Post-resolution customer satisfaction rating on 1-5 scale (1=Very Dissatisfied, 5=Very Satisfied).
      Collected via survey with ~70% response rate, only for resolved tickets.
      Distribution skewed toward positive (avg ~3.7): scores weighted toward 4-5 range.
      Null values (~40% overall) represent non-responses or non-resolved tickets.
    checks:
      - name: positive
  - name: AgentEmployeeKey
    type: INTEGER
    description: |
      Foreign key to employees table identifying the support agent handling the ticket.
      All tickets assigned to agents from support department (employee keys 1651-1950).
      Used for workload distribution analysis, performance tracking, and capacity planning.
    checks:
      - name: not_null
  - name: extracted_at
    type: TIMESTAMP
    description: |-
      ETL pipeline timestamp (UTC) indicating when this record was loaded into the data warehouse.
      Used for incremental processing, data freshness monitoring, and deduplication logic.
      All records from same pipeline run have identical timestamp value.

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

NUM_TICKETS = 150000
CHANNELS = ["Phone", "Email", "Chat", "Social"]
CATEGORIES = ["Shipping", "Returns", "Product Quality", "Billing", "Account", "Technical"]
PRIORITIES = ["Critical", "High", "Medium", "Low"]
# Support agent employee keys (dept 7, approx range)
SUPPORT_EMP_KEYS = list(range(1651, 1951))


def materialize():
    seed_all(42)
    rng = np.random.default_rng(42)
    keys = load_contoso_keys()
    customer_keys = np.array(keys["customer_keys"])
    order_keys = np.array(keys["order_keys"])
    product_keys = np.array(keys["product_keys"])
    min_date, max_date = keys["date_range"]

    logger.info("Generating %d support tickets...", NUM_TICKETS)

    # Vectorized generation
    n = NUM_TICKETS
    customers = rng.choice(customer_keys, size=n)
    channels = rng.choice(CHANNELS, size=n, p=[0.25, 0.35, 0.30, 0.10])
    categories = rng.choice(CATEGORIES, size=n, p=[0.20, 0.15, 0.20, 0.15, 0.15, 0.15])
    priorities = rng.choice(PRIORITIES, size=n, p=[0.05, 0.15, 0.55, 0.25])
    agents = rng.choice(SUPPORT_EMP_KEYS, size=n)

    # Created dates spread across data range
    days_range = (max_date - min_date).days
    offsets = rng.integers(0, max(1, days_range), size=n)
    created_dates = [
        (min_date + timedelta(days=int(o))).date() for o in offsets
    ]

    # 80% of tickets reference an order
    order_mask = rng.random(size=n) < 0.80
    orders = np.where(order_mask, rng.choice(order_keys, size=n), None)

    # 70% reference a product
    product_mask = rng.random(size=n) < 0.70
    products = np.where(product_mask, rng.choice(product_keys, size=n), None)

    # Resolution: 85% resolved, 10% open, 5% escalated
    status_rolls = rng.random(size=n)
    statuses = np.where(
        status_rolls < 0.85, "Resolved",
        np.where(status_rolls < 0.95, "Open", "Escalated"),
    )

    # Resolution times: 1-72 hours (in days), resolved only
    resolution_days = rng.integers(0, 4, size=n)
    resolved_dates = [
        created_dates[i] + timedelta(days=int(resolution_days[i]))
        if statuses[i] == "Resolved" else None
        for i in range(n)
    ]

    # Satisfaction: 1-5, skewed toward 4, only for resolved tickets (70% rate)
    sat_scores = rng.choice([1, 2, 3, 4, 5], size=n, p=[0.05, 0.10, 0.20, 0.40, 0.25])
    # Only assign scores to resolved tickets with 70% response rate
    sat_mask = (statuses == "Resolved") & (rng.random(size=n) < 0.70)

    records = []
    for i in range(n):
        records.append({
            "SupportTicketKey": i + 1,
            "CustomerKey": int(customers[i]),
            "OrderKey": int(orders[i]) if orders[i] is not None else None,
            "ProductKey": int(products[i]) if products[i] is not None else None,
            "Channel": channels[i],
            "Category": categories[i],
            "Priority": priorities[i],
            "Status": statuses[i],
            "CreatedDate": created_dates[i],
            "ResolvedDate": resolved_dates[i],
            "SatisfactionScore": int(sat_scores[i]) if sat_mask[i] else None,
            "AgentEmployeeKey": int(agents[i]),
        })

    df = pd.DataFrame(records)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d support tickets", len(df))
    return df
