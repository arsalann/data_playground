"""@bruin

name: raw.wat_wikiproject_articles
description: |
  Extended-universe article list scraped from WikiProject assessment categories
  for five projects whose articles are systematically missing from
  Vital-Articles-Level-4. WikiProject Companies and WikiProject Brands target
  named-entity companies (the user-flagged gap); WikiProject Computing and
  WikiProject Internet culture target software/products/internet topics;
  WikiProject Business adds business/economics concepts.

  Subject mapping (priority order shapes which WikiProject "wins" for
  multi-project articles):
    1. WikiProject Companies         -> Society and social sciences / Companies (extended)
    2. WikiProject Brands            -> Society and social sciences / Brands (extended)
    3. WikiProject Computing         -> Technology / Computing (extended)
    4. WikiProject Internet culture  -> Technology / Internet culture (extended)
    5. WikiProject Business          -> Society and social sciences / Business and economics (extended)

  Note the case-sensitive MediaWiki category naming: WikiProject Companies
  uses the lowercase `company` in its category names
  (`Category:Top-importance company articles`), not `Companies`.

  Importance filter: Top + High only. Mid/Low importance would explode the
  universe with noise (e.g. small regional companies).

  Source: MediaWiki Action API `list=categorymembers` against each project's
  assessment categories (e.g. `Category:Top-importance Computing articles`).
  Category members are Talk:* pages; we strip the "Talk:" prefix to recover
  the article title.

  Dedupe rules:
    1. Within this asset: an article that appears in multiple WikiProjects
       keeps its first-seen project/subject mapping (deterministic order:
       Business, Computing, Internet culture).
    2. Against Vital Articles: articles already in `raw.wat_vital_articles`
       are EXCLUDED — the dashboard joins this extended set as an additive
       layer, not a re-classification of Vital articles.

  Output:
    universe_tier = 'wikiproject_extended' on every row, so downstream
    queries can keep Vital and extended cleanly separable.

  Run cost: ~30-60 API requests (one paginated `categorymembers` call per
  (project, importance) pair plus continuations). Completes in ~2-3 min
  with the 1s polite delay between requests.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace
image: python:3.11

depends:
  - raw.wat_vital_articles

columns:
  - name: article_title
    type: STRING
    description: Canonical Wikipedia article title (Talk-namespace prefix stripped).
    primary_key: true
  - name: subject
    type: STRING
    description: Top-level subject mapped to the existing 11-subject schema.
  - name: sub_subject
    type: STRING
    description: Second-tier label, suffixed with the marker "extended" to distinguish from Vital sub-subjects.
  - name: wikiproject
    type: STRING
    description: Source WikiProject name.
  - name: importance
    type: STRING
    description: WikiProject importance class (Top or High).
  - name: universe_tier
    type: STRING
    description: Always wikiproject_extended; joins back to the universe staging layer.
  - name: ingested_at
    type: TIMESTAMP
    description: UTC timestamp when this row was scraped.

@bruin"""

import logging
import os
import random
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from google.cloud import bigquery

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

API_URL = "https://en.wikipedia.org/w/api.php"
USER_AGENT = "wikipedia-ai-trends/0.1 (arsalan.noorafkan@getbruin.com)"
PROJECT_ID = "bruin-playground-arsalan"
VITAL_TABLE = f"{PROJECT_ID}.raw.wat_vital_articles"

REQUEST_DELAY = float(os.environ.get("WAT_WP_DELAY", "1.0"))  # seconds between API calls

# (display_project_name, importance_class, category_title, subject, sub_subject)
# Ordered by priority: when an article is in multiple projects the first
# entry wins. Companies and Brands come before Business so that a named-
# entity company (e.g. Netflix) is tagged "Companies (extended)" rather
# than buried under the generic "Business and economics (extended)" bucket.
PROJECTS = [
    # Companies — named-entity companies (the user-flagged gap)
    ("WikiProject Companies",        "Top",  "Top-importance company articles",                "Society and social sciences", "Companies (extended)"),
    ("WikiProject Companies",        "High", "High-importance company articles",               "Society and social sciences", "Companies (extended)"),
    # Brands — products and brands (significant overlap with Companies)
    ("WikiProject Brands",           "Top",  "Top-importance Brands articles",                 "Society and social sciences", "Brands (extended)"),
    ("WikiProject Brands",           "High", "High-importance Brands articles",                "Society and social sciences", "Brands (extended)"),
    # Computing — software, hardware, computer science (excluding what is in Vital)
    ("WikiProject Computing",        "Top",  "Top-importance Computing articles",              "Technology",                  "Computing (extended)"),
    ("WikiProject Computing",        "High", "High-importance Computing articles",             "Technology",                  "Computing (extended)"),
    # Internet culture — internet-era topics
    ("WikiProject Internet culture", "Top",  "Top-importance Internet culture articles",       "Technology",                  "Internet culture (extended)"),
    ("WikiProject Internet culture", "High", "High-importance Internet culture articles",      "Technology",                  "Internet culture (extended)"),
    # Business — accounting, finance, economics concepts (generic; lowest priority)
    ("WikiProject Business",         "Top",  "Top-importance WikiProject Business articles",   "Society and social sciences", "Business and economics (extended)"),
    ("WikiProject Business",         "High", "High-importance WikiProject Business articles",  "Society and social sciences", "Business and economics (extended)"),
]


def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"})
    return s


def api_get(session: requests.Session, params: dict) -> dict:
    """Polite GET with exponential backoff on 429/503/maxlag."""
    params = dict(params)
    params.setdefault("maxlag", 5)
    delay = 1.0
    last_exc: Exception | None = None
    for attempt in range(10):
        try:
            r = session.get(API_URL, params=params, timeout=60)
            if r.status_code == 200:
                try:
                    data = r.json()
                except ValueError as e:
                    last_exc = e
                    time.sleep(delay + random.random())
                    delay = min(delay * 2, 60)
                    continue
                if isinstance(data, dict) and data.get("error", {}).get("code") == "maxlag":
                    retry_after = float(r.headers.get("Retry-After", 5))
                    time.sleep(retry_after + random.random())
                    continue
                if "error" in data:
                    raise RuntimeError(f"API error: {data['error']}")
                if REQUEST_DELAY > 0:
                    time.sleep(REQUEST_DELAY)
                return data
            if r.status_code in (429, 503):
                retry_after = float(r.headers.get("Retry-After", delay * 2))
                logger.warning("HTTP %d (attempt %d), sleep %.1fs", r.status_code, attempt + 1, retry_after)
                time.sleep(retry_after + random.random())
                delay = min(delay * 2, 60)
                continue
            logger.warning("HTTP %d on %s", r.status_code, params.get("cmtitle"))
        except requests.RequestException as e:
            last_exc = e
            logger.warning("Network error attempt %d: %s", attempt + 1, e)
        time.sleep(delay + random.random())
        delay = min(delay * 2, 60)
    raise RuntimeError(f"API failed after retries: {last_exc}")


def fetch_category_titles(session: requests.Session, category: str) -> list[str]:
    """Return article titles (Talk: stripped) for one assessment category."""
    titles: list[str] = []
    cmcontinue: str | None = None
    pages_fetched = 0
    while True:
        params = {
            "action": "query",
            "list": "categorymembers",
            "cmtitle": f"Category:{category}",
            "cmlimit": 500,
            "cmprop": "title",
            "cmnamespace": 1,  # talk pages only
            "format": "json",
        }
        if cmcontinue:
            params["cmcontinue"] = cmcontinue
        data = api_get(session, params)
        members = data.get("query", {}).get("categorymembers", []) or []
        for m in members:
            t = m.get("title", "")
            if t.startswith("Talk:"):
                titles.append(t[len("Talk:"):])
        pages_fetched += 1
        cont = data.get("continue", {})
        cmcontinue = cont.get("cmcontinue")
        if not cmcontinue:
            break
        if pages_fetched > 100:
            logger.warning("Pagination safety stop on %s", category)
            break
    return titles


def load_vital_titles(bq: bigquery.Client) -> set[str]:
    q = f"SELECT DISTINCT article_title FROM `{VITAL_TABLE}`"
    return {r.article_title for r in bq.query(q).result()}


def materialize():
    session = get_session()
    bq = bigquery.Client(project=PROJECT_ID)

    vital_titles = load_vital_titles(bq)
    logger.info("Loaded %d Vital-Articles titles for dedupe", len(vital_titles))

    seen: dict[str, dict] = {}  # title -> first-seen row dict
    raw_counts: dict[tuple[str, str], int] = {}

    for project, importance, category, subject, sub_subject in PROJECTS:
        logger.info("Scraping Category:%s", category)
        titles = fetch_category_titles(session, category)
        raw_counts[(project, importance)] = len(titles)
        logger.info("  → %d titles from %s (%s)", len(titles), project, importance)
        for t in titles:
            if t in seen:
                continue
            if t in vital_titles:
                continue
            seen[t] = {
                "article_title": t,
                "subject": subject,
                "sub_subject": sub_subject,
                "wikiproject": project,
                "importance": importance,
            }

    logger.info("Raw fetch totals by project/importance: %s", raw_counts)
    logger.info("Unique extended articles after dedupe (vs Vital + intra-list): %d", len(seen))

    df = pd.DataFrame(list(seen.values()))
    if df.empty:
        # Return an empty-but-typed frame so the table is still created
        df = pd.DataFrame({
            "article_title": pd.Series(dtype="string"),
            "subject": pd.Series(dtype="string"),
            "sub_subject": pd.Series(dtype="string"),
            "wikiproject": pd.Series(dtype="string"),
            "importance": pd.Series(dtype="string"),
        })
    df["universe_tier"] = "wikiproject_extended"
    df["ingested_at"] = datetime.now(timezone.utc)
    return df
