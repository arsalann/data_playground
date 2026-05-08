"""@bruin

name: contoso_v2_raw.campaigns
description: |
  Marketing campaigns for Contoso v2, 2010-01-01 to 2026-05-01. ~700 campaigns
  clustered around real retail events (Black Friday, Cyber Monday,
  Back-to-School, Mother's Day, Valentine's, etc.) with name templates like
  "BlackFriday 2018 - Email (Nov)". Near-zero campaigns 2020-03 to 2020-05
  (ad freeze). 2.5-3x cadence 2023-2024 (AI product launches). Budgets are
  bimodal — $5-15K always-on plus $50-500K seasonal pushes; AI-boom ad-spend
  multiplier amplifies budgets in 2023-2024. 70% target a specific product
  (with a tilt toward AI brands post-2023); 30% are brand awareness.
connection: gcp-default
tags:
  - marketing
  - campaigns
  - dimension_table

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: campaign_key
    type: INTEGER
    primary_key: true
    description: Sparse campaign identifier.
    checks:
      - name: not_null
      - name: unique
  - name: campaign_name
    type: VARCHAR
    description: Human-readable campaign name (theme - channel - month).
    checks:
      - name: not_null
  - name: theme
    type: VARCHAR
    description: Campaign theme anchor (BlackFriday, BackToSchool, BrandAwareness, ProductLaunch, etc.).
    checks:
      - name: not_null
  - name: channel
    type: VARCHAR
    description: Email, Paid Search, Social, Display, or Referral.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Email
          - Paid Search
          - Social
          - Display
          - Referral
  - name: start_date
    type: DATE
    description: Campaign launch date.
    checks:
      - name: not_null
  - name: end_date
    type: DATE
    description: Campaign end date.
    checks:
      - name: not_null
  - name: budget_amount
    type: NUMERIC
    description: Planned budget in USD.
    checks:
      - name: not_null
      - name: non_negative
  - name: spend_amount
    type: NUMERIC
    description: Actual spend in USD (typically 70-110% of budget).
    checks:
      - name: not_null
      - name: non_negative
  - name: currency
    type: VARCHAR
    description: Always USD.
    checks:
      - name: not_null
  - name: target_segment
    type: VARCHAR
    description: All Customers, New Customers, Returning Customers, VIP, or Lapsed.
    checks:
      - name: not_null
  - name: product_key
    type: INTEGER
    description: Product the campaign targets, NULL for brand awareness.
  - name: extracted_at
    type: TIMESTAMP

@bruin"""

import logging
import os
from datetime import datetime, timezone

import sys; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _contoso_helpers import build_campaigns

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)


def materialize():
    df = build_campaigns().copy()
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d campaigns", len(df))
    return df
