"""@bruin
name: imdb_mutual_raw.imdb_cast
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Billed acting credits for the popular-title universe, from the IMDb
  Non-Commercial Datasets. Streams three official IMDb TSV exports:
    - title.ratings.tsv.gz    (to rebuild the same vote gate as imdb_titles)
    - title.principals.tsv.gz (billed cast/crew per title)
    - name.basics.tsv.gz      (person id -> display name)

  Only acting credits are kept (category in actor, actress) and only for
  titles above VOTE_THRESHOLD votes. IMPORTANT LIMITATION: title.principals
  lists just the top-billed principals per title (typically up to 10), not the
  full cast. Shared-cast counts therefore measure overlap of *leading* casts,
  which understates total shared appearances but keeps the signal on
  headline actors.

  Data source: https://datasets.imdbws.com/ (IMDb Non-Commercial Datasets)
  License: IMDb Non-Commercial Licensing (personal / non-commercial use only).
  Not affiliated with or endorsed by IMDb.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: tconst
    type: STRING
    description: IMDb title identifier
  - name: ordering
    type: INTEGER
    description: IMDb billing order within the title (lower = higher billed)
  - name: nconst
    type: STRING
    description: IMDb person identifier
  - name: actor_name
    type: STRING
    description: Person display name (primaryName from name.basics)
  - name: category
    type: STRING
    description: Credit category (actor or actress)
  - name: characters
    type: STRING
    description: Character(s) played, as recorded by IMDb (nullable)
  - name: extracted_at
    type: TIMESTAMP
    description: Ingestion timestamp

@bruin"""

import gzip
import io
import json
from datetime import datetime, timezone

import pandas as pd
import requests

RATINGS_URL = "https://datasets.imdbws.com/title.ratings.tsv.gz"
PRINCIPALS_URL = "https://datasets.imdbws.com/title.principals.tsv.gz"
NAMES_URL = "https://datasets.imdbws.com/name.basics.tsv.gz"

VOTE_THRESHOLD = 25000
ACTING_CATEGORIES = {"actor", "actress"}


def _stream_lines(url):
    with requests.get(url, stream=True, timeout=1800) as resp:
        resp.raise_for_status()
        with gzip.GzipFile(fileobj=resp.raw) as gz:
            text = io.TextIOWrapper(gz, encoding="utf-8", newline="")
            header = text.readline().rstrip("\n").split("\t")
            idx = {name: i for i, name in enumerate(header)}
            for line in text:
                yield idx, line.rstrip("\n").split("\t")


def _clean(value):
    return None if value in (None, "", "\\N") else value


def _parse_characters(raw):
    raw = _clean(raw)
    if raw is None:
        return None
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return ", ".join(str(p) for p in parsed) or None
    except (ValueError, TypeError):
        pass
    return raw


def popular_tconsts():
    keep = set()
    for idx, row in _stream_lines(RATINGS_URL):
        votes = row[idx["numVotes"]]
        if votes not in ("", "\\N") and int(votes) >= VOTE_THRESHOLD:
            keep.add(row[idx["tconst"]])
    print(f"Popular tconsts (votes>={VOTE_THRESHOLD}): {len(keep)}")
    return keep


def materialize():
    keep_titles = popular_tconsts()

    credits = []
    needed_names = set()
    scanned = 0
    for idx, row in _stream_lines(PRINCIPALS_URL):
        scanned += 1
        if scanned % 10_000_000 == 0:
            print(f"  principals scanned: {scanned:,}")
        if row[idx["category"]] not in ACTING_CATEGORIES:
            continue
        tconst = row[idx["tconst"]]
        if tconst not in keep_titles:
            continue
        nconst = row[idx["nconst"]]
        needed_names.add(nconst)
        credits.append(
            {
                "tconst": tconst,
                "ordering": int(row[idx["ordering"]]),
                "nconst": nconst,
                "category": row[idx["category"]],
                "characters": _parse_characters(row[idx["characters"]]),
            }
        )
    print(f"Acting credits kept: {len(credits):,} across {len(needed_names):,} people")

    names = {}
    for idx, row in _stream_lines(NAMES_URL):
        nconst = row[idx["nconst"]]
        if nconst in needed_names:
            names[nconst] = row[idx["primaryName"]]
            if len(names) == len(needed_names):
                break
    print(f"Names resolved: {len(names):,}")

    df = pd.DataFrame.from_records(credits)
    df["actor_name"] = df["nconst"].map(names)
    df["extracted_at"] = datetime.now(timezone.utc)
    df = df[
        ["tconst", "ordering", "nconst", "actor_name", "category", "characters", "extracted_at"]
    ]
    return df


if __name__ == "__main__":
    materialize().to_parquet("/tmp/imdb_cast.parquet", index=False)
