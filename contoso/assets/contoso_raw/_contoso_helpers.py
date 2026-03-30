"""Shared utilities for Contoso pipeline assets.

Downloads and caches Contoso V2 Parquet files from GitHub releases,
provides key lookups for synthetic data generators, and seeded RNG helpers.

NOT a Bruin asset (prefixed with _).
"""

import logging
import os
import random
import subprocess
import tempfile

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import requests

logger = logging.getLogger(__name__)

RELEASE_URL = (
    "https://github.com/sql-bi/Contoso-Data-Generator-V2-Data"
    "/releases/download/ready-to-use-data/parquet-1m.7z"
)
CACHE_DIR = os.path.join(tempfile.gettempdir(), "contoso_cache")

PARQUET_FILES = {
    "sales": "sales.parquet",
    "orders": "orders.parquet",
    "order_rows": "orderrows.parquet",
    "customers": "customer.parquet",
    "products": "product.parquet",
    "stores": "store.parquet",
    "dates": "date.parquet",
    "currency_exchange": "currencyexchange.parquet",
}


def _ensure_extracted():
    """Download and extract the Contoso V2 parquet archive if not cached."""
    archive_path = os.path.join(CACHE_DIR, "parquet-1m.7z")
    marker = os.path.join(CACHE_DIR, ".extracted")

    if os.path.exists(marker):
        return

    os.makedirs(CACHE_DIR, exist_ok=True)

    if not os.path.exists(archive_path):
        logger.info("Downloading Contoso V2 parquet archive (1M scale)...")
        resp = requests.get(RELEASE_URL, stream=True, timeout=300)
        resp.raise_for_status()
        with open(archive_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(
            "Downloaded %.1f MB", os.path.getsize(archive_path) / 1024 / 1024
        )

    logger.info("Extracting archive...")
    subprocess.run(
        ["7z", "x", "-y", "-o" + CACHE_DIR, archive_path],
        check=True,
        capture_output=True,
    )

    with open(marker, "w") as f:
        f.write("ok")
    logger.info("Extraction complete")


def load_parquet(table_name: str) -> pd.DataFrame:
    """Load a Contoso V2 parquet table by logical name.

    Args:
        table_name: One of: sales, orders, order_rows, customers,
                    products, stores, dates, currency_exchange
    """
    _ensure_extracted()
    filename = PARQUET_FILES[table_name]
    path = os.path.join(CACHE_DIR, filename)
    df = pq.read_table(path).to_pandas()
    logger.info("Loaded %s: %d rows", table_name, len(df))
    return df


_keys_cache = {}


def load_contoso_keys() -> dict:
    """Return a dict of valid keys from the Contoso V2 data.

    Returns dict with:
        customer_keys: list of CustomerKey values
        product_keys: list of ProductKey values
        store_keys: list of StoreKey values
        order_keys: list of OrderKey values
        order_dates: dict mapping OrderKey -> order date
        product_prices: dict mapping ProductKey -> (Cost, Price)
        store_countries: dict mapping StoreKey -> CountryName
        date_range: (min_date, max_date) tuple
    """
    if _keys_cache:
        return _keys_cache

    _ensure_extracted()

    customers = load_parquet("customers")
    _keys_cache["customer_keys"] = customers["CustomerKey"].tolist()

    products = load_parquet("products")
    _keys_cache["product_keys"] = products["ProductKey"].tolist()
    _keys_cache["product_prices"] = dict(
        zip(products["ProductKey"], zip(products["Cost"], products["Price"]))
    )

    stores = load_parquet("stores")
    _keys_cache["store_keys"] = stores["StoreKey"].tolist()
    _keys_cache["store_countries"] = dict(
        zip(stores["StoreKey"], stores["CountryName"])
    )

    orders = load_parquet("orders")
    _keys_cache["order_keys"] = orders["OrderKey"].tolist()
    _keys_cache["order_dates"] = dict(
        zip(orders["OrderKey"], pd.to_datetime(orders["DT"]))
    )

    dates = load_parquet("dates")
    date_col = pd.to_datetime(dates["Date"])
    _keys_cache["date_range"] = (date_col.min(), date_col.max())

    logger.info(
        "Loaded keys: %d customers, %d products, %d stores, %d orders",
        len(_keys_cache["customer_keys"]),
        len(_keys_cache["product_keys"]),
        len(_keys_cache["store_keys"]),
        len(_keys_cache["order_keys"]),
    )
    return _keys_cache


def get_seeded_faker(seed: int = 42):
    """Return a deterministically-seeded Faker instance."""
    from faker import Faker

    fake = Faker()
    Faker.seed(seed)
    return fake


def get_seeded_rng(seed: int = 42) -> np.random.Generator:
    """Return a deterministic numpy RNG."""
    return np.random.default_rng(seed)


def seed_all(seed: int = 42):
    """Seed all random generators for reproducibility."""
    random.seed(seed)
    np.random.seed(seed)
