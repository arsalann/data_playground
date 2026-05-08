"""@bruin

name: contoso_v2_raw.campaign_attribution
description: |
  Multi-touch campaign attribution for Contoso v2. Approximately 40% of orders
  are attributed to one or more marketing campaigns. Touchpoints fall on or
  after the campaign start_date and within end_date + 30d, and 1-30 days
  before the order date. Touch counts: 50% single-touch, 30% two-touch,
  15% three-touch, 5% 4+ touch (concentrated on high-AOV journeys). Each
  touchpoint flagged as is_first_touch / is_last_touch.
connection: gcp-default
instance: b1.large
tags:
  - marketing
  - attribution
  - fact_table

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: attribution_key
    type: INTEGER
    primary_key: true
    description: Sparse attribution record identifier.
    checks:
      - name: not_null
      - name: unique
  - name: order_key
    type: INTEGER
    description: Foreign key to orders.
    checks:
      - name: not_null
  - name: campaign_key
    type: INTEGER
    description: Foreign key to campaigns.
    checks:
      - name: not_null
  - name: touchpoint_date
    type: DATE
    description: Date of the campaign touchpoint (1-30d before order_date).
    checks:
      - name: not_null
  - name: channel
    type: VARCHAR
    description: Channel of the touchpoint, inherited from campaign.
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
    description: True for the earliest touchpoint in the journey.
    checks:
      - name: not_null
  - name: is_last_touch
    type: BOOLEAN
    description: True for the latest touchpoint before the order.
    checks:
      - name: not_null
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
    build_campaigns, build_orders, get_seeded_rng, sparse_keys,
)

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)

ATTRIBUTION_RATE = 0.40


def materialize():
    rng = get_seeded_rng(139)
    orders = build_orders()
    campaigns = build_campaigns()

    order_keys = orders["order_key"].values
    order_dates = pd.to_datetime(orders["order_date"]).values

    # Pre-build campaign arrays
    camp_keys = campaigns["campaign_key"].values
    camp_starts = pd.to_datetime(campaigns["start_date"]).values
    camp_ends = pd.to_datetime(campaigns["end_date"]).values
    camp_channels = campaigns["channel"].values

    # Bucket campaigns by start year-month for fast lookup
    n_attributed = int(len(order_keys) * ATTRIBUTION_RATE)
    pick_idx = rng.choice(len(order_keys), size=n_attributed, replace=False)
    pick_idx.sort()

    logger.info(
        "Attributing %d / %d orders to campaigns",
        n_attributed, len(order_keys),
    )

    sparse_iter = iter(sparse_keys(2_000_000, gap_rate=0.04, start=1, seed=139))
    rows = []

    # Use days from campaign start as the lookup pivot
    camp_start_d64 = camp_starts.astype("datetime64[D]")
    camp_end_d64 = camp_ends.astype("datetime64[D]")

    for ix in pick_idx:
        ok = int(order_keys[ix])
        odt = order_dates[ix]
        if pd.isna(odt):
            continue
        odt_d64 = np.datetime64(pd.Timestamp(odt).date(), "D")

        # Eligible: start <= order_date <= end + 30d
        elig_mask = (camp_start_d64 <= odt_d64) & (
            (camp_end_d64 + np.timedelta64(30, "D")) >= odt_d64
        )
        elig_idx = np.flatnonzero(elig_mask)
        if len(elig_idx) == 0:
            continue

        # Touch count distribution
        r = rng.random()
        if r < 0.50:
            n_touches = 1
        elif r < 0.80:
            n_touches = 2
        elif r < 0.95:
            n_touches = 3
        else:
            n_touches = int(rng.integers(4, 7))
        n_touches = min(n_touches, len(elig_idx))

        chosen = rng.choice(elig_idx, size=n_touches, replace=False)
        # Sort touchpoints chronologically for first/last flags
        days_before = sorted(
            [int(rng.integers(1, 31)) for _ in range(n_touches)],
            reverse=True,
        )
        for j, c_idx in enumerate(chosen):
            touch_date = (pd.Timestamp(odt).date()
                          - timedelta(days=days_before[j]))
            # Don't put touch before campaign start
            cstart = pd.Timestamp(camp_starts[c_idx]).date()
            if touch_date < cstart:
                touch_date = cstart
            rows.append({
                "attribution_key": int(next(sparse_iter)),
                "order_key": ok,
                "campaign_key": int(camp_keys[c_idx]),
                "touchpoint_date": touch_date,
                "channel": str(camp_channels[c_idx]),
                "is_first_touch": j == 0,
                "is_last_touch": j == n_touches - 1,
            })

    df = pd.DataFrame(rows)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d attribution records", len(df))
    return df
