"""@bruin

name: raw.wat_vital_articles
description: |
  Wikipedia Vital Articles (Level 4) — the ~10,000-article curated universe used
  as the population for this analysis. Each article carries the editorially-assigned
  subject (e.g., "Technology", "Arts") and sub-subject (e.g., "Agriculture",
  "Painting"), so the dashboard can break down AI-reference patterns by domain
  without doing ad-hoc category-tree traversal.

  Source: scraped from `Wikipedia:Vital articles/Level 4/<subject>` subpages via
  the MediaWiki Action API (prop=wikitext), then list-items are parsed for
  `[[ArticleTitle]]` wikilinks under each heading level. The eleven subject
  subpages are:
    People, History, Geography, Arts, Philosophy and religion, Everyday life,
    Society and social sciences, Biology and health sciences, Physical sciences,
    Technology, Mathematics.

  Run cost: ~11 API requests, completes in <1 minute. Idempotent (create+replace).
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: article_title
    type: STRING
    description: Canonical Wikipedia article title (with underscores → spaces).
    primary_key: true
  - name: subject
    type: STRING
    description: Top-level Vital Articles subject (one of the eleven).
  - name: sub_subject
    type: STRING
    description: First-level sub-section heading within the subject page.
  - name: sub_sub_subject
    type: STRING
    description: Second-level sub-section heading, when present (else null).
  - name: ingested_at
    type: TIMESTAMP
    description: UTC timestamp when this snapshot was scraped.

@bruin"""

import logging
import os
import re
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

SUBJECTS = [
    "People",
    "History",
    "Geography",
    "Arts",
    "Philosophy and religion",
    "Everyday life",
    "Society and social sciences",
    "Biology and health sciences",
    "Physical sciences",
    "Technology",
    "Mathematics",
]

# Match list items like:  # {{Icon|B}} [[Article]]      or
#                         # {{Icon|GA}} '''[[Article]]''' ([[Wikipedia:Vital articles/Level 3|Level 3]])
#                         #: [[Article]]   (sub-bullets in some sub-lists)
LIST_ITEM_LINK = re.compile(
    r"^\s*#+:?\s*"             # one-or-more "#", optional ":"
    r"(?:\{\{[^}]+\}\}\s*)*"   # optional {{Icon|..}} or other templates
    r"'{0,3}\s*\[\["           # optional bold quotes, then [[
    r"([^\]|#]+?)"             # capture: page name (no | or #)
    r"(?:\#[^\]|]*)?"          # optional #anchor (discarded)
    r"(?:\|[^\]]*)?"           # optional |display text (discarded)
    r"\]\]"                    # ]]
)

HEADING = re.compile(r"^(={2,5})\s*(.+?)\s*\1\s*$")
SPAN_ANCHOR = re.compile(r"<span[^>]*></span>", re.IGNORECASE)


def fetch_wikitext(page_title: str) -> str:
    params = {
        "action": "parse",
        "page": page_title,
        "prop": "wikitext",
        "format": "json",
        "redirects": 1,
    }
    headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"}
    for attempt in range(4):
        try:
            r = requests.get(API_URL, params=params, headers=headers, timeout=60)
            if r.status_code == 200:
                data = r.json()
                wt = data.get("parse", {}).get("wikitext", {}).get("*", "")
                if wt:
                    return wt
            logger.warning("HTTP %d on %s (attempt %d)", r.status_code, page_title, attempt + 1)
        except requests.RequestException as e:
            logger.warning("Network error on %s attempt %d: %s", page_title, attempt + 1, e)
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"Failed to fetch {page_title}")


def parse_subject_page(subject: str, wikitext: str) -> list[dict]:
    rows: list[dict] = []
    sub_subject: str | None = None
    sub_sub: str | None = None
    seen_titles: set[str] = set()

    for raw_line in wikitext.splitlines():
        line = raw_line.rstrip()
        h = HEADING.match(line)
        if h:
            level = len(h.group(1))
            name = SPAN_ANCHOR.sub("", h.group(2)).strip()
            # Skip top-level subject heading (always == Technology == etc.)
            if level == 2:
                # First level-2 inside the page IS the subject (e.g., "Technology")
                # We only treat level-2 as sub_subject if it's not the subject itself.
                if name.lower() == subject.lower():
                    sub_subject = None
                else:
                    sub_subject = name
                sub_sub = None
            elif level == 3:
                sub_sub = name
            elif level >= 4:
                # Deeper headings overwrite sub_sub for context
                sub_sub = name
            continue

        m = LIST_ITEM_LINK.match(line)
        if not m:
            continue
        title_raw = m.group(1).strip()
        title = title_raw.replace("_", " ")
        # Skip non-article links (e.g., Wikipedia:, File:, Category:, Special:)
        if ":" in title.split(" ")[0] and title.split(":")[0] in {
            "Wikipedia", "File", "Category", "Special", "Help", "Portal",
            "Template", "User", "Talk", "Image", "WP", "m",
        }:
            continue
        # Deduplicate within a page
        if title in seen_titles:
            continue
        seen_titles.add(title)
        rows.append({
            "article_title": title,
            "subject": subject,
            "sub_subject": sub_subject,
            "sub_sub_subject": sub_sub,
        })
    return rows


def materialize():
    all_rows: list[dict] = []
    for subject in SUBJECTS:
        page = f"Wikipedia:Vital articles/Level 4/{subject}"
        logger.info("Scraping %s", page)
        wt = fetch_wikitext(page)
        rows = parse_subject_page(subject, wt)
        logger.info("  -> %d articles in %s", len(rows), subject)
        all_rows.extend(rows)
        time.sleep(0.5)  # be polite

    df = pd.DataFrame(all_rows)
    # Cross-subject dedupe (some articles legitimately appear in multiple subjects;
    # keep the first occurrence so each article maps to one canonical subject).
    df = df.drop_duplicates(subset=["article_title"], keep="first").reset_index(drop=True)
    df["ingested_at"] = datetime.now(timezone.utc)
    logger.info("Total unique articles: %d across %d subjects", len(df), df["subject"].nunique())
    return df
