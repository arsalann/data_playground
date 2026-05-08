"""@bruin

name: contoso_v2_raw.ad_spend_daily
description: |
  Daily ad spend and performance per campaign for Contoso v2,
  2010-01-01 to 2026-05-01. One row per campaign-day across the campaign's
  active window. Daily spend = budget/duration with 50-150% pacing variation
  and a 5x lift on Black Friday/Cyber Monday. Q4 brings a +30% CTR/conversion
  lift. Channel-specific CTR/conv/CPM benchmarks: Email 2.5%/4%/$5,
  Paid Search 3.5%/3%/$15, Social 1.2%/1.5%/$8, Display 0.8%/1%/$4,
  Referral 4.5%/5%/$3. ~2% tracking errors (clicks > impressions or NULL
  conversions).
connection: gcp-default
instance: b1.medium
tags:
  - marketing
  - advertising
  - fact_table

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: ad_spend_key
    type: INTEGER
    primary_key: true
    description: Sparse identifier per campaign-day.
    checks:
      - name: not_null
      - name: unique
  - name: campaign_key
    type: INTEGER
    description: Foreign key to campaigns.
    checks:
      - name: not_null
  - name: spend_date
    type: DATE
    description: Date of spend (within campaign window).
    checks:
      - name: not_null
  - name: channel
    type: VARCHAR
    description: Inherited from parent campaign.
    checks:
      - name: not_null
  - name: impressions
    type: INTEGER
    description: Ad impressions served.
    checks:
      - name: not_null
      - name: non_negative
  - name: clicks
    type: INTEGER
    description: Ad clicks received.
    checks:
      - name: not_null
      - name: non_negative
  - name: conversions
    type: INTEGER
    description: Conversions attributed to the campaign-day. NULL for ~1% tracking errors.
  - name: spend_amount
    type: NUMERIC
    description: Daily spend in USD.
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
  - name: extracted_at
    type: TIMESTAMP

@bruin"""

import logging
import os
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

import sys; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _contoso_helpers import (
    CHANNEL_METRICS, PIPELINE_END, build_campaigns, get_seeded_rng,
    holiday_name, is_holiday_window, sparse_keys,
)

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)


def materialize():
    rng = get_seeded_rng(137)
    campaigns = build_campaigns()
    logger.info("Building daily ad spend for %d campaigns", len(campaigns))

    sparse_iter = iter(sparse_keys(500_000, gap_rate=0.04, start=1, seed=137))

    rows = []
    for _, c in campaigns.iterrows():
        camp_key = int(c["campaign_key"])
        channel = c["channel"]
        metrics = CHANNEL_METRICS[channel]
        start = pd.to_datetime(c["start_date"]).date()
        end = pd.to_datetime(c["end_date"]).date()
        if end > PIPELINE_END:
            end = PIPELINE_END
        if start > end:
            continue
        duration = max(1, (end - start).days + 1)
        spend_total = float(c["spend_amount"])
        daily_avg = spend_total / duration

        cur = start
        while cur <= end:
            # Pacing variation 50-150%
            pace = float(rng.uniform(0.5, 1.5))
            # 5x spike on BF/CM, 1.4x in holiday window otherwise
            hname = holiday_name(cur)
            if hname == "Black Friday":
                pace *= 5.0
            elif hname == "Cyber Monday":
                pace *= 4.0
            elif is_holiday_window(cur):
                pace *= 1.30
            daily_spend = round(daily_avg * pace, 2)

            # Q4 CTR/conv lift
            ctr_lift = 1.30 if cur.month in (11, 12) else 1.0

            impressions = int(daily_spend / metrics["cpm"] * 1000)
            clicks = int(impressions * metrics["ctr"] * ctr_lift
                         * float(rng.uniform(0.7, 1.3)))
            conversions = int(clicks * metrics["conv_rate"] * ctr_lift
                              * float(rng.uniform(0.5, 1.5)))

            # 2% tracking errors
            err = rng.random()
            if err < 0.01:
                clicks = max(clicks, impressions + int(rng.integers(1, 50)))
            elif err < 0.02:
                conversions_out = None
            else:
                conversions_out = max(0, conversions)

            if err >= 0.02:
                conversions_out = max(0, conversions)

            rows.append({
                "ad_spend_key": int(next(sparse_iter)),
                "campaign_key": camp_key,
                "spend_date": cur,
                "channel": channel,
                "impressions": max(0, impressions),
                "clicks": max(0, clicks),
                "conversions": conversions_out,
                "spend_amount": daily_spend,
                "currency": "USD",
            })
            cur += timedelta(days=1)

    df = pd.DataFrame(rows)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d ad spend rows", len(df))
    return df
