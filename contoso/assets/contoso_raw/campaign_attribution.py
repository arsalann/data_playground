"""@bruin

name: contoso_raw.campaign_attribution
description: |
  Marketing attribution fact table that tracks the customer journey from campaign touchpoints to purchase
  for Contoso Electronics. This table implements multi-touch attribution modeling to link marketing campaigns
  to order conversions, providing critical data for campaign effectiveness analysis and marketing ROI calculations.

  This synthetic dataset models realistic attribution patterns where approximately 40% of orders (678K records)
  can be traced back to specific marketing campaigns through customer touchpoints. Each attributed order typically
  has 1-3 touchpoints spanning up to 30 days before purchase, representing common digital marketing customer journeys
  across email marketing, paid search, social media, display advertising, and referral channels.

  Key business characteristics:
  - Multi-touch attribution: Orders can have multiple campaign touchpoints before conversion
  - Cross-channel journey tracking: Customers may interact with multiple channels before purchasing
  - First/last touch attribution flags: Supports both attribution models for marketing analysis
  - 30-day attribution window: Touchpoints captured up to 30 days before order date
  - Channel distribution matches typical e-commerce patterns across 5 marketing channels
  - Deterministic generation (seed=42) ensures reproducible testing and development scenarios

  Marketing insights and analytics use cases:
  - Campaign ROAS (Return on Ad Spend) calculation by linking spend to attributed revenue
  - Multi-touch attribution analysis to understand channel interaction effects
  - Customer journey mapping across touchpoints and conversion funnels
  - Marketing mix modeling for budget allocation optimization
  - First-click vs last-click attribution comparison for campaign credit assignment

  Data relationships and lineage:
  - Links to contoso_raw.orders via order_key for revenue attribution and customer context
  - Links to contoso_raw.campaigns via campaign_key for campaign metadata, budget, and spend data
  - TouchpointDate ranges from 2016-2025, aligning with the full business dataset timespan
  - Used downstream in contoso_staging.marketing_performance for aggregated campaign metrics

  Source: Synthetic data generated with Python Faker, deterministic with seed=42
connection: bruin-playground-eu
instance: b1.large
tags:
  - marketing
  - attribution
  - customer_journey
  - multi_touch
  - fact_table
  - campaign_analytics
  - synthetic
  - cross_channel

materialization:
  type: table
  strategy: create+replace
image: python:3.11


columns:
  - name: attribution_key
    type: INTEGER
    description: |
      Unique attribution record identifier (primary key). Sequential integers starting from 1 for each
      touchpoint-to-order mapping. This serves as the grain of the attribution table where each record
      represents one customer interaction with one campaign that contributed to an order conversion.
      Used for deduplication and ensuring referential integrity in downstream attribution analysis.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: order_key
    type: INTEGER
    description: |
      Foreign key to contoso_raw.orders table linking the attribution record to the specific order
      that was influenced by this campaign touchpoint. One order can have multiple attribution records
      (multi-touch attribution), but each attribution record maps to exactly one order. Essential for
      calculating attributed revenue and order-level conversion metrics. Ranges from ~5M to ~40M
      reflecting the order key distribution in the orders table.
    checks:
      - name: not_null
  - name: campaign_key
    type: INTEGER
    description: |
      Foreign key to contoso_raw.campaigns table identifying which specific marketing campaign generated
      this customer touchpoint. Links to campaign metadata including budget, channel, targeting, and
      creative details. Values range 1-200 corresponding to the 200 campaigns in the master data.
      Critical for campaign-level ROI analysis and budget allocation decisions.
    checks:
      - name: not_null
  - name: touchpoint_date
    type: DATE
    description: |
      Date when the customer interaction with the campaign occurred (YYYY-MM-DD format).
      Always 1-30 days before the associated order date to reflect realistic consideration periods
      in consumer electronics purchasing. Spans from 2016-04-18 to 2025-12-30 across the full
      dataset timespan. Used for attribution window analysis and customer journey timing insights.
    checks:
      - name: not_null
  - name: channel
    type: VARCHAR
    description: |
      Marketing channel through which the customer touchpoint occurred. One of five channels:
      Email, Paid Search, Social, Display, or Referral. This dimension enables channel-level
      performance analysis and cross-channel attribution studies. Channel distribution reflects
      typical e-commerce marketing mix with paid search and email being prominent channels.
      Maximum length is 11 characters ("Paid Search").
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Email
          - Paid Search
          - Social
          - Display
          - Referral
  - name: is_first_touch
    type: BOOLEAN
    description: |
      Attribution flag indicating whether this touchpoint was the first campaign interaction
      in the customer's journey toward this specific order. TRUE for approximately 58% of records
      (392,266 out of 678,223), as many orders have only one touchpoint or this represents the
      initial campaign exposure. Used in first-touch attribution models to assign full conversion
      credit to the awareness-generating campaign.
    checks:
      - name: not_null
  - name: is_last_touch
    type: BOOLEAN
    description: |
      Attribution flag indicating whether this touchpoint was the final campaign interaction
      before the customer completed their purchase. TRUE for approximately 58% of records,
      identical to is_first_touch distribution since many customer journeys involve single
      touchpoints. Critical for last-touch attribution models that assign conversion credit
      to the campaign that directly drove the purchase decision.
    checks:
      - name: not_null
  - name: extracted_at
    type: TIMESTAMP
    description: |
      ETL timestamp recording when this attribution record was generated and loaded into the
      data warehouse (UTC timezone). Consistent across all records in each batch load, used for
      data lineage tracking, freshness monitoring, and version control. Essential for understanding
      data recency and coordinating downstream processing schedules.
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

CHANNELS = ["Email", "Paid Search", "Social", "Display", "Referral"]
NUM_CAMPAIGNS = 200
ATTRIBUTION_RATE = 0.40  # 40% of orders attributed to campaigns


def materialize():
    seed_all(42)
    rng = np.random.default_rng(42)
    keys = load_contoso_keys()
    order_keys = keys["order_keys"]
    order_dates = keys["order_dates"]
    min_date, max_date = keys["date_range"]

    # Build campaign date ranges with same seed pattern
    campaign_rng = np.random.default_rng(42)
    campaign_ranges = []
    for i in range(NUM_CAMPAIGNS):
        channel = campaign_rng.choice(CHANNELS)
        campaign_rng.choice(["All Customers"] * 5)
        campaign_rng.choice(["Holiday Sale"] * 12)
        days_range = (max_date - min_date).days - 90
        start_offset = int(campaign_rng.integers(0, max(1, days_range)))
        start_date = (min_date + timedelta(days=start_offset)).date()
        duration = int(campaign_rng.integers(7, 90))
        end_date = start_date + timedelta(days=duration)
        campaign_rng.uniform(5_000, 200_000)
        campaign_rng.uniform(0.7, 1.1)
        campaign_rng.random()
        if campaign_rng.random() < 0.7:
            pass
        campaign_ranges.append({
            "CampaignKey": i + 1,
            "Channel": channel,
            "StartDate": pd.Timestamp(start_date),
            "EndDate": pd.Timestamp(end_date),
        })

    # Select ~40% of orders to attribute
    n_attributed = int(len(order_keys) * ATTRIBUTION_RATE)
    attributed_orders = rng.choice(order_keys, size=n_attributed, replace=False)

    logger.info(
        "Generating attribution for %d orders (%.0f%% of %d)...",
        n_attributed, ATTRIBUTION_RATE * 100, len(order_keys),
    )

    records = []
    attr_key = 0

    for order_key in attributed_orders:
        order_date = order_dates[order_key]
        if pd.isna(order_date):
            continue

        order_dt = pd.Timestamp(order_date).tz_localize(None)

        # Find campaigns active around order date
        eligible = [
            c for c in campaign_ranges
            if c["StartDate"] <= order_dt <= c["EndDate"] + pd.Timedelta(days=30)
        ]
        if not eligible:
            # Pick a random campaign
            eligible = [campaign_ranges[int(rng.integers(0, len(campaign_ranges)))]]

        # 1-3 touchpoints per attributed order
        n_touches = int(rng.choice([1, 2, 3], p=[0.4, 0.35, 0.25]))
        selected_campaigns = rng.choice(eligible, size=min(n_touches, len(eligible)), replace=True)

        for idx, camp in enumerate(selected_campaigns):
            attr_key += 1
            days_before = int(rng.integers(1, 31))
            touch_date = (order_dt - timedelta(days=days_before)).date()

            records.append({
                "AttributionKey": attr_key,
                "OrderKey": int(order_key),
                "CampaignKey": int(camp["CampaignKey"]),
                "TouchpointDate": touch_date,
                "Channel": camp["Channel"],
                "IsFirstTouch": idx == 0,
                "IsLastTouch": idx == len(selected_campaigns) - 1,
            })

    df = pd.DataFrame(records)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d attribution records", len(df))
    return df
