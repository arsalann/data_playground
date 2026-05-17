"""@bruin

name: raw.wat_ai_seed_articles
description: |
  Curated list of ~50 Wikipedia articles that constitute the "AI" target set.
  Every wikilink from a universe article to any of these targets (or to one of
  their redirect aliases) counts as an "AI reference" in the analysis.

  The list spans the AI landscape:
    * Foundational concepts (Artificial intelligence, Machine learning, Deep
      learning, Neural network, Artificial neural network)
    * Architectures (Transformer, Convolutional neural network, Recurrent
      neural network, Generative adversarial network, Diffusion model)
    * Modern systems (GPT-4, GPT-3, ChatGPT, Large language model, Generative
      artificial intelligence, BERT, Stable Diffusion, DALL-E, Midjourney)
    * Companies and orgs frequently cited (OpenAI, DeepMind, Anthropic,
      Hugging Face)
    * Sub-fields (Natural language processing, Computer vision, Reinforcement
      learning, Speech recognition, Expert system)
    * Cultural/policy (AI alignment, AI safety, AI ethics, AGI, Existential
      risk from AGI, Technological singularity)

  For each seed title, we hit the MediaWiki API to (a) resolve to its canonical
  title (so misspellings/redirects fold together at join time), (b) fetch the
  earliest-known page-creation date so downstream queries can correctly handle
  the "this article didn't exist yet in 2019" case.

  Companion redirect-aliases column captures redirects pointing TO each canonical
  AI article — these are extra strings to match against wikilinks in old
  revisions, since editors sometimes typed `[[GPT-4 (LLM)]]` or `[[OpenAI GPT-3]]`
  before redirects collapsed them.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: canonical_title
    type: STRING
    description: Canonical Wikipedia article title for this AI target.
    primary_key: true
  - name: seed_title
    type: STRING
    description: Original title we queried with (may differ from canonical).
  - name: category
    type: STRING
    description: Manual grouping (Foundations, Architectures, Modern systems, Companies, Subfields, Cultural/policy).
  - name: created_at
    type: TIMESTAMP
    description: Earliest revision timestamp for the article (UTC).
  - name: redirect_aliases
    type: STRING
    description: Pipe-separated list of redirect titles that resolve to this article.
  - name: ingested_at
    type: TIMESTAMP
    description: UTC timestamp when this snapshot was scraped.

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

API_URL = "https://en.wikipedia.org/w/api.php"
USER_AGENT = "wikipedia-ai-trends/0.1 (arsalan.noorafkan@getbruin.com)"

SEED_LIST: list[tuple[str, str]] = [
    # (seed_title, category)
    # Foundations
    ("Artificial intelligence", "Foundations"),
    ("Machine learning", "Foundations"),
    ("Deep learning", "Foundations"),
    ("Neural network (machine learning)", "Foundations"),
    ("Artificial neural network", "Foundations"),
    ("Supervised learning", "Foundations"),
    ("Unsupervised learning", "Foundations"),
    ("Self-supervised learning", "Foundations"),
    # Architectures
    ("Transformer (deep learning architecture)", "Architectures"),
    ("Convolutional neural network", "Architectures"),
    ("Recurrent neural network", "Architectures"),
    ("Generative adversarial network", "Architectures"),
    ("Diffusion model", "Architectures"),
    ("Attention (machine learning)", "Architectures"),
    ("Long short-term memory", "Architectures"),
    # Modern systems / models
    ("GPT-4", "Modern systems"),
    ("GPT-3", "Modern systems"),
    ("ChatGPT", "Modern systems"),
    ("Large language model", "Modern systems"),
    ("Generative artificial intelligence", "Modern systems"),
    ("BERT (language model)", "Modern systems"),
    ("Stable Diffusion", "Modern systems"),
    ("DALL-E", "Modern systems"),
    ("Midjourney", "Modern systems"),
    ("Claude (language model)", "Modern systems"),
    ("Gemini (language model)", "Modern systems"),
    ("LLaMA", "Modern systems"),
    ("Foundation model", "Modern systems"),
    # Companies / organizations
    ("OpenAI", "Companies"),
    ("DeepMind", "Companies"),
    ("Anthropic", "Companies"),
    ("Hugging Face", "Companies"),
    # Subfields
    ("Natural language processing", "Subfields"),
    ("Computer vision", "Subfields"),
    ("Reinforcement learning", "Subfields"),
    ("Speech recognition", "Subfields"),
    ("Expert system", "Subfields"),
    ("Symbolic artificial intelligence", "Subfields"),
    ("Machine translation", "Subfields"),
    ("Robotics", "Subfields"),
    # Cultural / policy
    ("AI alignment", "Cultural/policy"),
    ("AI safety", "Cultural/policy"),
    ("Ethics of artificial intelligence", "Cultural/policy"),
    ("Artificial general intelligence", "Cultural/policy"),
    ("Existential risk from artificial general intelligence", "Cultural/policy"),
    ("Technological singularity", "Cultural/policy"),
    ("Regulation of artificial intelligence", "Cultural/policy"),
    ("AI winter", "Cultural/policy"),
]


def api_get(params: dict) -> dict:
    headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"}
    for attempt in range(4):
        try:
            r = requests.get(API_URL, params=params, headers=headers, timeout=60)
            if r.status_code == 200:
                return r.json()
            logger.warning("HTTP %d (attempt %d) on %s", r.status_code, attempt + 1, params)
        except requests.RequestException as e:
            logger.warning("Network error attempt %d: %s", attempt + 1, e)
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"API failed for params={params}")


def resolve_and_enrich(seed_title: str) -> dict:
    """Resolve to canonical title via redirects, fetch creation timestamp."""
    # Step 1: resolve canonical title via 'info' with redirects=1
    data = api_get({
        "action": "query",
        "titles": seed_title,
        "prop": "info",
        "format": "json",
        "redirects": 1,
    })
    pages = data.get("query", {}).get("pages", {})
    if not pages:
        return {"canonical_title": None}
    page = next(iter(pages.values()))
    if page.get("missing") is not None:
        logger.warning("Page missing: %s", seed_title)
        return {"canonical_title": None}
    canonical = page["title"]

    # Step 2: earliest revision timestamp = page creation date
    data2 = api_get({
        "action": "query",
        "titles": canonical,
        "prop": "revisions",
        "rvprop": "timestamp",
        "rvlimit": 1,
        "rvdir": "newer",
        "format": "json",
    })
    pages2 = data2.get("query", {}).get("pages", {})
    page2 = next(iter(pages2.values()))
    revs = page2.get("revisions", [])
    created_ts = revs[0]["timestamp"] if revs else None

    # Step 3: redirects that resolve TO this canonical title
    aliases: list[str] = []
    cont: dict = {}
    while True:
        params = {
            "action": "query",
            "titles": canonical,
            "prop": "redirects",
            "rdlimit": "max",
            "format": "json",
        }
        params.update(cont)
        data3 = api_get(params)
        pages3 = data3.get("query", {}).get("pages", {})
        page3 = next(iter(pages3.values()))
        for rd in page3.get("redirects", []) or []:
            aliases.append(rd["title"])
        if "continue" in data3:
            cont = data3["continue"]
        else:
            break

    return {
        "canonical_title": canonical,
        "created_at": created_ts,
        "redirect_aliases": "|".join(sorted(set(aliases))),
    }


def materialize():
    rows: list[dict] = []
    for seed, category in SEED_LIST:
        logger.info("Resolving %s", seed)
        info = resolve_and_enrich(seed)
        if not info.get("canonical_title"):
            continue
        rows.append({
            "canonical_title": info["canonical_title"],
            "seed_title": seed,
            "category": category,
            "created_at": info.get("created_at"),
            "redirect_aliases": info.get("redirect_aliases") or "",
        })
        time.sleep(0.2)

    df = pd.DataFrame(rows)
    # Keep first occurrence if two seeds resolve to the same canonical.
    df = df.drop_duplicates(subset=["canonical_title"], keep="first").reset_index(drop=True)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    df["ingested_at"] = datetime.now(timezone.utc)
    logger.info("Resolved %d AI seed articles", len(df))
    return df
