"""@bruin

name: contoso_raw.campaigns
description: |
  Marketing campaign master data for Contoso consumer electronics retailer.
  Contains campaign metadata across 5 marketing channels (Email, Paid Search, Social, Display, Referral)
  with budget/spend tracking and product targeting. Approximately 70% of campaigns target specific products,
  while 30% are brand awareness campaigns. Synthetic data generated with deterministic patterns (seed=42)
  to simulate realistic campaign planning and execution patterns. Campaign names follow the convention:
  "[Campaign Type] - [Channel] - [Month Year]" (e.g., "Holiday Sale - Email - Dec 2023").

  Data spans 2016-2026 with campaigns typically running 7-90 days. Budget ranges from $5K-$200K
  with actual spend varying ±30% from budget. Used downstream in marketing performance analytics
  to calculate ROAS, CPA, and channel effectiveness metrics.
connection: gcp-default
tags:
  - marketing
  - campaigns
  - synthetic
  - master_data
  - dimension_table

materialization:
  type: table
  strategy: create+replace
image: python:3.11


columns:
  - name: campaign_key
    type: INTEGER
    description: |
      Unique campaign identifier (primary key). Sequential integers starting from 1.
      Used as foreign key in campaign_attribution and ad_spend_daily tables for performance tracking.
    primary_key: true
  - name: campaign_name
    type: VARCHAR
    description: |
      Human-readable campaign name following pattern: "[Campaign Type] - [Channel] - [Month Year]".
      Campaign types include Holiday Sale, Back to School, New Product Launch, Flash Sale, etc.
      Almost unique (3 duplicates in 200 records due to same type/channel/month combinations).
  - name: channel
    type: VARCHAR
    description: |
      Marketing channel (dimension). One of: Email, Paid Search, Social, Display, Referral.
      Used for channel-level ROI analysis and budget allocation decisions.
  - name: start_date
    type: DATE
    description: |
      Campaign launch date. Ranges from 2016-01-05 to 2026-09-28 across the full dataset timespan.
      Used with end_date to calculate campaign duration and performance windows.
  - name: end_date
    type: DATE
    description: |
      Campaign end date. Always after start_date, with durations ranging 7-90 days.
      Used for performance window analysis and budget pacing calculations.
  - name: budget_amount
    type: DOUBLE
    description: |
      Planned campaign budget in USD. Ranges from $5,101 to $198,403 with realistic distribution
      based on campaign type and channel. Used for budget vs actual spend variance analysis.
  - name: spend_amount
    type: DOUBLE
    description: |
      Actual campaign spend in USD. Typically 70-110% of budget_amount, can exceed budget
      for high-performing campaigns. Used in ROAS calculations and spend efficiency analysis.
  - name: currency
    type: VARCHAR
    description: |
      Currency code for budget and spend amounts. Always 'USD' in current dataset.
      Included for potential multi-currency expansion.
  - name: target_segment
    type: VARCHAR
    description: |
      Customer segmentation target (dimension). One of: All Customers, New Customers,
      Returning Customers, High Value, Lapsed. Used for campaign personalization and
      customer lifecycle analysis.
  - name: product_key
    type: INTEGER
    description: |
      Foreign key to products table for product-specific campaigns. NULL for brand awareness
      campaigns (approximately 30% of records). Links to contoso_raw.products for product
      performance attribution and category-level campaign analysis.
  - name: extracted_at
    type: TIMESTAMP
    description: |
      ETL timestamp when campaign data was loaded (UTC). Single timestamp per batch load.
      Used for data lineage tracking and freshness monitoring.

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

CHANNELS = ["Email", "Paid Search", "Social", "Display", "Referral"]
SEGMENTS = ["All Customers", "New Customers", "Returning Customers", "High Value", "Lapsed"]
CAMPAIGN_TYPES = [
    "Holiday Sale", "Back to School", "Summer Clearance", "New Product Launch",
    "Flash Sale", "Loyalty Reward", "Brand Awareness", "Category Promotion",
    "Seasonal Campaign", "Anniversary Sale", "Weekend Special", "Bundle Deal",
]

NUM_CAMPAIGNS = 200


def materialize():
    seed_all(42)
    rng = np.random.default_rng(42)
    keys = load_contoso_keys()
    product_keys = keys["product_keys"]
    min_date, max_date = keys["date_range"]

    logger.info("Generating %d campaigns...", NUM_CAMPAIGNS)

    records = []
    for i in range(NUM_CAMPAIGNS):
        camp_key = i + 1
        channel = rng.choice(CHANNELS)
        segment = rng.choice(SEGMENTS)
        camp_type = rng.choice(CAMPAIGN_TYPES)

        # Random start within data range
        days_range = (max_date - min_date).days - 90
        start_offset = int(rng.integers(0, max(1, days_range)))
        start_date = (min_date + timedelta(days=start_offset)).date()
        duration = int(rng.integers(7, 90))
        end_date = start_date + timedelta(days=duration)

        budget = round(rng.uniform(5_000, 200_000), 2)
        spend = round(budget * rng.uniform(0.7, 1.1), 2)

        # 70% of campaigns target a specific product, 30% are brand campaigns
        product_key = None
        if rng.random() < 0.7:
            product_key = int(rng.choice(product_keys))

        name = f"{camp_type} - {channel} - {start_date.strftime('%b %Y')}"

        records.append({
            "CampaignKey": camp_key,
            "CampaignName": name,
            "Channel": channel,
            "StartDate": start_date,
            "EndDate": end_date,
            "BudgetAmount": budget,
            "SpendAmount": spend,
            "Currency": "USD",
            "TargetSegment": segment,
            "ProductKey": product_key,
        })

    df = pd.DataFrame(records)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d campaigns", len(df))
    return df
