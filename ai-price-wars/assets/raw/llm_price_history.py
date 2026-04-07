"""@bruin

name: raw.llm_price_history
description: |
  Historical pricing data for major LLM providers tracking the "intelligence deflation"
  phenomenon - how AI model costs decrease over time through competitive market dynamics.
  Each record represents a pricing period for a specific model with temporal boundaries.

  Covers 10+ major providers (OpenAI, Anthropic, Google, Amazon, Mistral, Cohere, etc.)
  with 103+ unique models. Most records (99%+) represent current pricing; historical
  price changes are rare as this market is relatively new. Essential foundation data
  for AI price wars analysis and cost trend forecasting.

  Data characteristics:
  - Heavily skewed toward current pricing (only ~1% historical records)
  - Input pricing ranges from $0.035 to $150 per million tokens
  - Output pricing typically 2-10x higher than input (up to $600/Mtok for premium models)
  - Cached input pricing available for ~45% of models (significant cost optimization)
  - Some models show unusual pricing patterns (negative values indicate credits/promotions)

  This is the only known free source providing true historical pricing time-series
  for LLM cost analysis, making it invaluable for market intelligence.

  Source: https://www.llm-prices.com/historical-v1.json (maintained by Simon Willison)
connection: bruin-playground-arsalan
tags:
  - ai-price-wars
  - pricing-data
  - intelligence-deflation
  - llm-market
  - historical-data
  - external-api
  - cost-analysis
  - competitive-intelligence

materialization:
  type: table
  strategy: append
image: python:3.11

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: model_id
    type: VARCHAR
    description: |
      Unique model identifier serving as natural key (e.g. gpt-4, claude-3-opus,
      llama-2-70b). Format varies by vendor but typically provider-model-variant.
      103 distinct values tracked. Used for joining with quality benchmarks and
      current model catalogs.
    primary_key: true
    checks:
      - name: not_null
  - name: vendor
    type: VARCHAR
    description: |
      AI provider/company name (openai, anthropic, google, amazon, mistral, cohere,
      meta, etc.). 10 distinct vendors currently tracked. Normalized to lowercase.
      Critical dimension for competitive analysis and market concentration metrics.
    checks:
      - name: not_null
  - name: model_name
    type: VARCHAR
    description: |
      Human-readable model display name as marketed by provider. Often includes
      capability tiers (turbo, pro, ultra) or version numbers. 104 distinct names.
      More descriptive than model_id for user interfaces and reporting.
  - name: price_input_per_mtok
    type: DOUBLE
    description: |
      Cost per million input tokens in USD for this pricing period. Ranges from
      $0.035 (commodity models) to $150 (premium/specialized models). Average ~$5.67.
      Foundation metric for cost-per-query calculations and price elasticity analysis.
    checks:
      - name: not_null
  - name: price_output_per_mtok
    type: DOUBLE
    description: |
      Cost per million output tokens in USD for this pricing period. Typically 2-10x
      higher than input cost, reflecting generation complexity. Ranges from $0.04 to
      $600. Average ~$24.15. Critical for calculating total conversation costs.
    checks:
      - name: not_null
  - name: price_input_cached_per_mtok
    type: DOUBLE
    description: |
      Cost per million cached input tokens in USD (available for ~45% of models).
      Significant optimization - typically 25-90% discount vs regular input pricing.
      Null when provider doesn't offer caching. Ranges from $0.005 to $37.50.
      Essential for cost optimization strategies.
  - name: from_date
    type: DATE
    description: |
      Start date of this pricing period. Null for 99%+ of records (original/launch
      pricing). When populated, indicates a historical price change event. Rare
      due to market maturity - most models launched with current pricing intact.
  - name: to_date
    type: DATE
    description: |
      End date of this pricing period. Null for current pricing (99%+ of records).
      When populated alongside from_date, marks a completed pricing period. Critical
      for time-series analysis of price evolution patterns.
  - name: is_current_price
    type: BOOLEAN
    description: |
      Flag indicating whether this represents currently active pricing (to_date is null).
      True for 99%+ of records. False value indicates historical pricing no longer
      in effect. Key filter for current market analysis vs historical comparisons.
    checks:
      - name: not_null
  - name: extracted_at
    type: TIMESTAMP
    description: |
      Timestamp when this data was fetched from the llm-prices.com API. All records
      in current dataset extracted simultaneously. Used for data freshness validation
      and deduplication logic in downstream staging. Import metadata - do not modify.
    checks:
      - name: not_null

@bruin"""

import logging
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

HISTORICAL_URL = "https://www.llm-prices.com/historical-v1.json"
MAX_RETRIES = 5


def fetch_with_retry(url, retries=MAX_RETRIES):
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=60)
            if resp.status_code in (429, 502, 503):
                wait = 15 * (attempt + 1)
                logger.warning("HTTP %d, retrying in %ds", resp.status_code, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            wait = 10 * (attempt + 1)
            logger.warning("Request error (attempt %d/%d): %s", attempt + 1, retries, e)
            time.sleep(wait)
    logger.error("All %d retries exhausted for %s", retries, url)
    return None


def materialize():
    logger.info("Fetching LLM historical pricing data")
    data = fetch_with_retry(HISTORICAL_URL)

    if not data or "prices" not in data:
        logger.error("Failed to fetch LLM price history")
        return pd.DataFrame()

    prices = data["prices"]
    logger.info("Fetched %d pricing records", len(prices))

    rows = []
    for p in prices:
        rows.append({
            "model_id": p.get("id", ""),
            "vendor": p.get("vendor", ""),
            "model_name": p.get("name", ""),
            "price_input_per_mtok": p.get("input"),
            "price_output_per_mtok": p.get("output"),
            "price_input_cached_per_mtok": p.get("input_cached"),
            "from_date": p.get("from_date"),
            "to_date": p.get("to_date"),
            "is_current_price": p.get("to_date") is None,
        })

    df = pd.DataFrame(rows)
    df["from_date"] = pd.to_datetime(df["from_date"], errors="coerce").dt.date
    df["to_date"] = pd.to_datetime(df["to_date"], errors="coerce").dt.date
    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info("Total pricing records: %d", len(df))
    logger.info("Vendors: %s", df["vendor"].value_counts().to_dict())
    logger.info("Current prices: %d, Historical: %d", df["is_current_price"].sum(), (~df["is_current_price"]).sum())

    return df
