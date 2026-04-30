"""@bruin

name: contoso_raw.ad_spend_daily
description: |
  Daily advertising spend and performance metrics for Contoso consumer electronics retailer.
  Contains granular daily performance data for each active campaign, including impressions, clicks,
  conversions, and spend amounts across 5 marketing channels (Email, Paid Search, Social, Display, Referral).

  Each record represents one campaign's performance on a single day, with metrics based on realistic
  channel-specific CTR and conversion rates. Email and Referral typically show higher engagement rates,
  while Display and Social have broader reach at lower CPMs. Paid Search delivers moderate performance
  at higher cost.

  Daily spend varies 50-150% of the campaign's average daily budget to simulate real-world pacing
  fluctuations. Performance metrics are calculated using deterministic channel-specific benchmarks:
  - Email: 2.5% CTR, 4% conversion rate, $5 CPM
  - Paid Search: 3.5% CTR, 3% conversion rate, $15 CPM
  - Social: 1.2% CTR, 1.5% conversion rate, $8 CPM
  - Display: 0.8% CTR, 1% conversion rate, $4 CPM
  - Referral: 4.5% CTR, 5% conversion rate, $3 CPM

  Data spans 2016-2026 with records only for active campaign days (no padding for inactive periods).
  Synthetic data generated with deterministic patterns (seed=42) to ensure reproducible results.
  Used downstream in marketing performance analysis, ROI calculations, and budget optimization models.
connection: contoso-duckdb
instance: b1.medium
tags:
  - marketing
  - advertising
  - performance_metrics
  - synthetic
  - fact_table
  - daily_grain

materialization:
  type: table
  strategy: create+replace
image: python:3.11


columns:
  - name: ad_spend_key
    type: INTEGER
    description: |
      Unique daily ad spend record identifier (primary key). Sequential integers starting from 1,
      incrementing for each campaign-day combination. Used for deduplication and row-level tracking
      in downstream analytics and reporting models.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: campaign_key
    type: INTEGER
    description: |
      Foreign key to campaigns table linking to campaign master data. References contoso_raw.campaigns.campaign_key
      for campaign metadata including budget, dates, target segment, and product focus. Used for
      campaign-level performance aggregation and attribution analysis.
    checks:
      - name: not_null
  - name: spend_date
    type: DATE
    description: |
      Date of advertising activity and spend (dimension). Always falls within the campaign's active
      date range (start_date to end_date). Used for time-series analysis, seasonal trending,
      and daily performance tracking. Ranges from 2016-01-18 to 2026-11-17 across active campaigns.
    checks:
      - name: not_null
  - name: channel
    type: VARCHAR
    description: |
      Marketing channel (dimension). One of: Email, Paid Search, Social, Display, Referral.
      Inherited from parent campaign for consistency. Used for channel-level ROI analysis,
      budget allocation optimization, and cross-channel performance comparison.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Email
          - Paid Search
          - Social
          - Display
          - Referral
  - name: impressions
    type: INTEGER
    description: |
      Number of ad impressions served (metric). Calculated as daily_spend / CPM * 1000, where CPM
      varies by channel ($3-$15). Higher for channels with premium inventory (Paid Search).
      Used for reach analysis and cost efficiency metrics. Always >= 0.
    checks:
      - name: not_null
  - name: clicks
    type: INTEGER
    description: |
      Number of ad clicks received (metric). Calculated as impressions * channel_CTR with ±30% daily
      variation. CTR ranges from 0.8% (Display) to 4.5% (Referral). Used for engagement analysis
      and click-through rate calculations. Always >= 0.
    checks:
      - name: not_null
  - name: conversions
    type: INTEGER
    description: |
      Number of conversions (purchases) attributed to this campaign-day (metric). Calculated as
      clicks * channel_conversion_rate with ±50% daily variation. Conversion rates range from 1% (Display)
      to 5% (Referral). Used for ROI calculations and campaign effectiveness analysis. Always >= 0.
    checks:
      - name: not_null
  - name: spend_amount
    type: DOUBLE
    description: |
      Daily advertising spend in USD (metric). Varies 50-150% of the campaign's average daily budget
      to simulate real-world pacing fluctuations and bidding dynamics. Used for budget tracking,
      cost analysis, and ROAS calculations. Always > 0 for active campaign days.
    checks:
      - name: not_null
  - name: currency
    type: VARCHAR
    description: |
      Currency code for spend amounts. Always 'USD' in current dataset but included for potential
      multi-currency expansion. Used for financial reporting and cross-market analysis standardization.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - USD
  - name: extracted_at
    type: TIMESTAMP
    description: |
      ETL extraction timestamp when daily ad spend data was loaded (UTC). Single timestamp per batch
      indicating data freshness and load time. Used for data lineage tracking, freshness monitoring,
      and incremental processing logic. All records share the same extraction timestamp per load.
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

# CTR and conversion rates by channel
CHANNEL_METRICS = {
    "Email": {"ctr": 0.025, "conv_rate": 0.04, "cpm": 5},
    "Paid Search": {"ctr": 0.035, "conv_rate": 0.03, "cpm": 15},
    "Social": {"ctr": 0.012, "conv_rate": 0.015, "cpm": 8},
    "Display": {"ctr": 0.008, "conv_rate": 0.01, "cpm": 4},
    "Referral": {"ctr": 0.045, "conv_rate": 0.05, "cpm": 3},
}

NUM_CAMPAIGNS = 200


def materialize():
    seed_all(42)
    rng = np.random.default_rng(42)
    keys = load_contoso_keys()
    min_date, max_date = keys["date_range"]

    # Regenerate campaign dates with same seed
    campaign_rng = np.random.default_rng(42)
    campaigns = []
    for i in range(NUM_CAMPAIGNS):
        channel = campaign_rng.choice(CHANNELS)
        campaign_rng.choice(["All Customers", "New Customers", "Returning Customers", "High Value", "Lapsed"])
        campaign_rng.choice(["Holiday Sale"] * 12)  # consume same random calls

        days_range = (max_date - min_date).days - 90
        start_offset = int(campaign_rng.integers(0, max(1, days_range)))
        start_date = (min_date + timedelta(days=start_offset)).date()
        duration = int(campaign_rng.integers(7, 90))
        end_date = start_date + timedelta(days=duration)

        budget = round(campaign_rng.uniform(5_000, 200_000), 2)
        spend = round(budget * campaign_rng.uniform(0.7, 1.1), 2)
        campaign_rng.random()  # consume product key roll
        if campaign_rng.random() < 0.7:  # consume product choice placeholder
            pass

        campaigns.append({
            "CampaignKey": i + 1,
            "Channel": channel,
            "StartDate": start_date,
            "EndDate": end_date,
            "TotalSpend": spend,
            "Duration": duration,
        })

    logger.info("Generating daily ad spend for %d campaigns...", len(campaigns))

    records = []
    ad_spend_key = 0

    for camp in campaigns:
        duration = camp["Duration"]
        daily_budget = camp["TotalSpend"] / max(1, duration)
        metrics = CHANNEL_METRICS[camp["Channel"]]

        current = camp["StartDate"]
        while current <= camp["EndDate"]:
            ad_spend_key += 1

            # Daily variation
            daily_spend = round(daily_budget * rng.uniform(0.5, 1.5), 2)
            impressions = int(daily_spend / metrics["cpm"] * 1000)
            clicks = int(impressions * metrics["ctr"] * rng.uniform(0.7, 1.3))
            conversions = int(clicks * metrics["conv_rate"] * rng.uniform(0.5, 1.5))

            records.append({
                "AdSpendKey": ad_spend_key,
                "CampaignKey": camp["CampaignKey"],
                "SpendDate": current,
                "Channel": camp["Channel"],
                "Impressions": max(0, impressions),
                "Clicks": max(0, clicks),
                "Conversions": max(0, conversions),
                "SpendAmount": daily_spend,
                "Currency": "USD",
            })

            current += timedelta(days=1)

    df = pd.DataFrame(records)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d daily ad spend records", len(df))
    return df
