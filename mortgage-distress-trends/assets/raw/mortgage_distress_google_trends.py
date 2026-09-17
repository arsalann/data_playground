"""@bruin
name: raw.mortgage_distress_google_trends
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  US Google Trends "interest over time" for search terms that signal difficulty
  paying a mortgage (foreclosure / loan-modification / mortgage-assistance
  intent), fetched monthly for 2004-01 to the present via the unofficial
  pytrends client. Each term is fetched on its own request, so its series is
  self-normalised to 0-100 where 100 is that term's single highest month across
  the whole 2004-present window (Google Trends' standard normalisation).

  Terms are grouped by `category`:
    - core_distress   : 5 unambiguous "I cannot pay my mortgage" queries. These
                        form the composite Mortgage Distress Search Index and all
                        peak during the 2008-2010 foreclosure crisis.
    - broad_foreclosure : the generic term "foreclosure" (mixes distressed
                        borrowers with investors hunting discounted homes).
    - covid_policy    : "mortgage forbearance" (peaks 2020, a policy-era term).
    - macro           : "unemployment benefits" (a general recession barometer).

  The current (partial) month is dropped so every row is a complete month.

  Data source: Google Trends (https://trends.google.com/trends) via pytrends 4.9.2
  License: Google Terms of Service. Interest values are relative indices, not
  absolute search counts.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: month
    type: DATE
    description: First day of the month (monthly Google Trends bucket)
    primary_key: true
  - name: term
    type: VARCHAR
    description: The Google search term queried
    primary_key: true
  - name: category
    type: VARCHAR
    description: "One of: core_distress, broad_foreclosure, covid_policy, macro"
  - name: interest
    type: INTEGER
    description: Relative search interest 0-100 (100 = the term's peak month, self-normalised over 2004-present)
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when the series was fetched from Google Trends

@bruin"""

import logging
import os
import random
import time
from datetime import datetime, timezone

import pandas as pd
from pytrends.request import TrendReq

# term -> category. Core distress terms are unambiguous "cannot pay" intent.
TERMS = {
    "foreclosure help": "core_distress",
    "loan modification": "core_distress",
    "stop foreclosure": "core_distress",
    "avoid foreclosure": "core_distress",
    "mortgage assistance": "core_distress",
    "foreclosure": "broad_foreclosure",
    "mortgage forbearance": "covid_policy",
    "unemployment benefits": "macro",
}

GEO = "US"

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _fetch_term(term: str, timeframe: str) -> pd.Series:
    """Fetch one term's monthly interest series, retrying on 429 rate limits."""
    for attempt in range(6):
        try:
            pt = TrendReq(hl="en-US", tz=360)
            pt.build_payload([term], timeframe=timeframe, geo=GEO)
            df = pt.interest_over_time()
            if df.empty:
                raise RuntimeError(f"empty series for {term!r}")
            # Drop the current partial month so every bucket is complete.
            df = df[~df["isPartial"].astype(bool)]
            return df[term]
        except Exception as exc:  # noqa: BLE001 - retry any transient trends error
            wait = 20 * (attempt + 1) + random.randint(0, 10)
            logger.warning(
                "%r attempt %d failed (%s: %s); waiting %ss",
                term,
                attempt + 1,
                type(exc).__name__,
                str(exc)[:80],
                wait,
            )
            time.sleep(wait)
    raise RuntimeError(f"Google Trends kept rate-limiting {term!r}; aborting")


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "2004-01-01").replace("T", " ").split()[0]
    end_date = os.environ.get(
        "BRUIN_END_DATE", datetime.now(timezone.utc).strftime("%Y-%m-%d")
    ).replace("T", " ").split()[0]
    timeframe = f"{start_date} {end_date}"
    logger.info("Fetching Google Trends data for %s through %s", start_date, end_date)

    rows = []
    for i, (term, category) in enumerate(TERMS.items()):
        series = _fetch_term(term, timeframe)
        logger.info(
            "%r category=%s n=%d peak=%d",
            term,
            category,
            len(series),
            int(series.max()),
        )
        for month, value in series.items():
            rows.append({
                "month": month.date(),
                "term": term,
                "category": category,
                "interest": int(value),
            })
        # Space out requests to stay under Google Trends rate limits.
        if i < len(TERMS) - 1:
            time.sleep(random.randint(10, 16))

    df = pd.DataFrame(rows)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Total rows: %d across %d terms", len(df), df["term"].nunique())
    return df
