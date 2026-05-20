"""Phase B: load DuckDB rows into BigQuery raw.wat_article_snapshots.

Run with VPN OFF (needs BigQuery access). Reads from
./wat_dec2019_backfill.duckdb and appends rows for snapshot_date=2019-12-01
that are not already present in BigQuery.
"""
from __future__ import annotations

import logging
import os
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
from google.cloud import bigquery

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT = "bruin-playground-arsalan"
DEST_TABLE = f"{PROJECT}.raw.wat_article_snapshots"
DUCKDB_PATH = Path(__file__).parent / "wat_dec2019_backfill.duckdb"
SNAPSHOT = date(2019, 12, 1)


def main():
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    df = con.execute("""
        SELECT article_title, revision_id, revision_timestamp,
               wikilinks, wikilinks_count, fetched_at
        FROM dec2019_snapshots
    """).fetchdf()
    logger.info("DuckDB rows: %d (valid revs: %d)", len(df), int(df["revision_id"].notna().sum()))

    bq = bigquery.Client(project=PROJECT)
    existing = {r.article_title for r in bq.query(
        f"SELECT article_title FROM `{DEST_TABLE}` WHERE snapshot_date = '2019-12-01'"
    ).result()}
    logger.info("Already in BigQuery: %d Dec-2019 rows", len(existing))

    df = df[~df["article_title"].isin(existing)].copy()
    logger.info("To insert: %d new rows", len(df))
    if df.empty:
        logger.info("Nothing to load.")
        return

    df["snapshot_date"] = SNAPSHOT
    df["revision_timestamp"] = pd.to_datetime(df["revision_timestamp"], errors="coerce", utc=True)
    df["fetched_at"] = pd.to_datetime(df["fetched_at"], errors="coerce", utc=True)
    df["revision_id"] = df["revision_id"].astype("Int64")
    df["wikilinks_count"] = df["wikilinks_count"].astype("Int64")
    df = df[[
        "article_title", "snapshot_date", "revision_id", "revision_timestamp",
        "wikilinks", "wikilinks_count", "fetched_at",
    ]]

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("article_title", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("snapshot_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("revision_id", "INT64"),
            bigquery.SchemaField("revision_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("wikilinks", "STRING"),
            bigquery.SchemaField("wikilinks_count", "INT64"),
            bigquery.SchemaField("fetched_at", "TIMESTAMP"),
        ],
    )
    job = bq.load_table_from_dataframe(df, DEST_TABLE, job_config=job_config)
    job.result()
    logger.info("Loaded %d rows into %s", len(df), DEST_TABLE)


if __name__ == "__main__":
    main()
