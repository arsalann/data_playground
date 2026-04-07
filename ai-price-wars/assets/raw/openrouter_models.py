"""@bruin

name: raw.openrouter_models
description: |
  Current snapshot of AI/LLM models available on OpenRouter, the world's largest
  multi-provider AI model marketplace. This is the foundation dataset for AI price
  wars analysis, capturing real-time pricing and capabilities across 350+ models
  from 50+ providers including frontier labs (OpenAI, Anthropic, Google), open
  source leaders (Meta, Mistral), and emerging players.

  Essential for analyzing "intelligence deflation" - the rapid decline in AI model
  costs over time. Feeds downstream analysis of price/performance ratios, market
  competition dynamics, and democratization of AI capabilities.

  Data freshness: Updated daily via public API (no auth required). Pricing can
  change frequently, especially during competitive pricing wars. Some providers
  use negative pricing to indicate promotional credits or special access tiers.

  Source: https://openrouter.ai/api/v1/models
connection: bruin-playground-arsalan
tags:
  - ai-market-data
  - pricing
  - llm-catalog
  - external-api
  - daily-refresh
  - ai-price-wars
  - raw-data

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
      OpenRouter model identifier in provider/model-name format (e.g. openai/gpt-4o,
      anthropic/claude-3-opus). Serves as primary key and canonical reference for
      joining with pricing history and performance benchmarks. 348 unique models
      currently tracked.
    primary_key: true
    checks:
      - name: not_null
  - name: model_name
    type: VARCHAR
    description: |
      Human-readable display name of the model as shown in OpenRouter UI. Often
      includes version numbers, capability hints, or marketing names. Used for
      fuzzy matching with Arena leaderboard rankings.
    checks:
      - name: not_null
  - name: provider
    type: VARCHAR
    description: |
      Model provider extracted from model_id prefix (e.g. openai, anthropic, google).
      56 distinct providers currently tracked, ranging from frontier labs to emerging
      startups. Essential for competitive analysis and market share calculations.
    checks:
      - name: not_null
  - name: price_input_per_mtok
    type: DOUBLE
    description: |
      Cost per million input tokens in USD. Core metric for pricing analysis.
      Negative values may indicate promotional credits, API errors, or special
      access tiers. Range: -1M to +150, with most models between $0.10-$30.
  - name: price_output_per_mtok
    type: DOUBLE
    description: |
      Cost per million output tokens in USD. Typically 2-10x higher than input
      pricing due to computational cost of generation. Critical for total cost
      of ownership calculations in downstream analysis.
  - name: context_length
    type: INTEGER
    description: |
      Maximum context window size in tokens. Key capability metric ranging from
      4K (older models) to 2M+ tokens (latest long-context models). Essential
      for use case classification and price/capability analysis.
    checks:
      - name: not_null
      - name: non_negative
  - name: max_completion_tokens
    type: INTEGER
    description: |
      Maximum output tokens the model can generate in a single response. Often
      limited to subset of context_length. Critical constraint for long-form
      content generation use cases. Note: Missing from current table structure
      but defined in code - may be null for many models.
  - name: model_created_at
    type: TIMESTAMP
    description: |
      Timestamp when the model was first made available on OpenRouter platform
      (converted from Unix timestamp). Not the model's training completion date.
      Used for analyzing model launch cadence and market evolution timeline.
    checks:
      - name: not_null
  - name: description
    type: VARCHAR
    description: |
      Provider-supplied model description including capabilities, use cases, and
      technical details. Length varies 67-330 chars. Essential for model
      categorization, capability analysis, and semantic search in catalogs.
    checks:
      - name: not_null
  - name: knowledge_cutoff
    type: VARCHAR
    description: |
      Training data cutoff date in YYYY-MM-DD format when available. Missing for
      36% of models (124/348 nulls). Critical for determining data freshness and
      model vintage for time-sensitive applications.
  - name: is_free
    type: BOOLEAN
    description: |
      Whether the model is completely free to use (both input and output pricing
      are zero). Currently 8% of models (28/348) are free, often open source
      models or promotional tiers. Key for cost optimization strategies.
    checks:
      - name: not_null
  - name: extracted_at
    type: TIMESTAMP
    description: |
      Timestamp when this data was fetched from OpenRouter API. All records in
      a batch share same timestamp, enabling point-in-time analysis and change
      detection across pipeline runs. Critical for data lineage and debugging.
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

OPENROUTER_API_URL = "https://openrouter.ai/api/v1/models"
MAX_RETRIES = 5


def fetch_with_retry(url, retries=MAX_RETRIES):
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=30)
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


def safe_float(val):
    if val is None or val == "" or val == "null":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def materialize():
    logger.info("Fetching OpenRouter model catalog")
    data = fetch_with_retry(OPENROUTER_API_URL)

    if not data or "data" not in data:
        logger.error("Failed to fetch OpenRouter models")
        return pd.DataFrame()

    models = data["data"]
    logger.info("Fetched %d models from OpenRouter", len(models))

    rows = []
    for m in models:
        model_id = m.get("id", "")
        provider = model_id.split("/")[0] if "/" in model_id else "unknown"

        pricing = m.get("pricing", {}) or {}
        price_input = safe_float(pricing.get("prompt"))
        price_output = safe_float(pricing.get("completion"))

        # Convert per-token to per-million-tokens
        price_input_mtok = price_input * 1_000_000 if price_input is not None else None
        price_output_mtok = price_output * 1_000_000 if price_output is not None else None

        created = m.get("created")
        created_dt = None
        if created:
            try:
                created_dt = datetime.fromtimestamp(created, tz=timezone.utc)
            except (ValueError, TypeError, OSError):
                pass

        rows.append({
            "model_id": model_id,
            "model_name": m.get("name", ""),
            "provider": provider,
            "price_input_per_mtok": price_input_mtok,
            "price_output_per_mtok": price_output_mtok,
            "context_length": m.get("context_length"),
            "max_completion_tokens": m.get("max_completion_tokens"),
            "model_created_at": created_dt,
            "description": (m.get("description", "") or "")[:4000],
            "knowledge_cutoff": m.get("knowledge_cutoff"),
            "is_free": (price_input_mtok == 0 and price_output_mtok == 0) if price_input_mtok is not None and price_output_mtok is not None else False,
        })

    df = pd.DataFrame(rows)
    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info("Total models: %d", len(df))
    logger.info("Providers: %s", df["provider"].nunique())
    logger.info("Free models: %d", df["is_free"].sum())

    return df
