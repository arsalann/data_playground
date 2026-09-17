"""@bruin
name: imdb_mutual_raw.imdb_titles
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Master list of well-known titles from the IMDb Non-Commercial Datasets.

  Downloads two official IMDb TSV exports and joins them in-memory:
    - title.ratings.tsv.gz  (averageRating, numVotes)
    - title.basics.tsv.gz   (titleType, primaryTitle, startYear, genres)

  Kept rows are the popular, non-adult feature titles that anchor the
  mutual-cast analysis: titleType in (movie, tvSeries, tvMiniSeries) with at
  least VOTE_THRESHOLD IMDb votes. The vote gate keeps the universe to a few
  thousand widely-recognised titles so shared-cast overlaps are meaningful
  rather than noise from obscure credits.

  Data source: https://datasets.imdbws.com/ (IMDb Non-Commercial Datasets)
  License: IMDb Non-Commercial Licensing (personal / non-commercial use only).
  Not affiliated with or endorsed by IMDb.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: tconst
    type: STRING
    description: IMDb title identifier (e.g. tt0133093)
    primary_key: true
  - name: title_type
    type: STRING
    description: IMDb title type (movie, tvSeries, tvMiniSeries)
  - name: primary_title
    type: STRING
    description: The more popular title / the title used on IMDb promotional materials
  - name: start_year
    type: INTEGER
    description: Release year (movie) or series start year
  - name: runtime_minutes
    type: INTEGER
    description: Primary runtime in minutes (nullable)
  - name: genres
    type: STRING
    description: Comma-separated IMDb genres (up to three)
  - name: average_rating
    type: FLOAT64
    description: IMDb weighted average user rating (1-10)
  - name: num_votes
    type: INTEGER
    description: Number of IMDb user votes backing the rating
  - name: extracted_at
    type: TIMESTAMP
    description: Ingestion timestamp

@bruin"""

import gzip
import io
from datetime import datetime, timezone

import pandas as pd
import requests

RATINGS_URL = "https://datasets.imdbws.com/title.ratings.tsv.gz"
BASICS_URL = "https://datasets.imdbws.com/title.basics.tsv.gz"

VOTE_THRESHOLD = 25000
TITLE_TYPES = {"movie", "tvSeries", "tvMiniSeries"}


def _stream_lines(url):
    """Yield decoded, tab-split rows from a gzipped TSV URL without buffering it all."""
    with requests.get(url, stream=True, timeout=600) as resp:
        resp.raise_for_status()
        with gzip.GzipFile(fileobj=resp.raw) as gz:
            text = io.TextIOWrapper(gz, encoding="utf-8", newline="")
            header = text.readline().rstrip("\n").split("\t")
            idx = {name: i for i, name in enumerate(header)}
            for line in text:
                yield idx, line.rstrip("\n").split("\t")


def _to_int(value):
    return int(value) if value not in (None, "", "\\N") else None


def load_popular_ratings():
    """Return {tconst: (average_rating, num_votes)} for titles above the vote gate."""
    ratings = {}
    for idx, row in _stream_lines(RATINGS_URL):
        votes = _to_int(row[idx["numVotes"]])
        if votes is not None and votes >= VOTE_THRESHOLD:
            ratings[row[idx["tconst"]]] = (float(row[idx["averageRating"]]), votes)
    print(f"Ratings above {VOTE_THRESHOLD} votes: {len(ratings)}")
    return ratings


def materialize():
    ratings = load_popular_ratings()

    records = []
    for idx, row in _stream_lines(BASICS_URL):
        tconst = row[idx["tconst"]]
        if tconst not in ratings:
            continue
        if row[idx["titleType"]] not in TITLE_TYPES:
            continue
        if row[idx["isAdult"]] == "1":
            continue
        avg_rating, num_votes = ratings[tconst]
        records.append(
            {
                "tconst": tconst,
                "title_type": row[idx["titleType"]],
                "primary_title": row[idx["primaryTitle"]],
                "start_year": _to_int(row[idx["startYear"]]),
                "runtime_minutes": _to_int(row[idx["runtimeMinutes"]]),
                "genres": None if row[idx["genres"]] == "\\N" else row[idx["genres"]],
                "average_rating": avg_rating,
                "num_votes": num_votes,
            }
        )

    df = pd.DataFrame.from_records(records)
    df["extracted_at"] = datetime.now(timezone.utc)
    print(f"Titles kept (movie/tvSeries/tvMiniSeries, votes>={VOTE_THRESHOLD}): {len(df)}")
    print(df["title_type"].value_counts().to_dict())
    return df


if __name__ == "__main__":
    materialize().to_parquet("/tmp/imdb_titles.parquet", index=False)
