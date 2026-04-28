"""dlt ingest pipeline for contoso-dbt.

Loads all 23 raw tables into BigQuery dataset `contoso_dbt_raw` by reusing
the existing Python asset logic in ../contoso/assets/contoso_raw/. Each
source file there exports a `materialize()` function that returns a
pandas DataFrame (Contoso V2 parquets for 8 tables, Faker-generated
synthetic data for 15 tables).

dlt's default snake_case naming convention normalizes the PascalCase
columns from the Contoso V2 parquets (OrderKey -> order_key, etc.) to
match the column names the dbt staging models expect.

Run:
    cd contoso-dbt
    export GOOGLE_APPLICATION_CREDENTIALS=../credentials/playground_key.json
    python ingest/pipeline.py
"""
from __future__ import annotations

import importlib.util
import logging
import os
import sys
from pathlib import Path

import dlt

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("contoso_dbt.ingest")

HERE = Path(__file__).resolve().parent
CONTOSO_RAW_DIR = HERE.parent.parent / "contoso" / "assets" / "contoso_raw"

# Make `_contoso_helpers` importable for the asset modules we load below.
if str(CONTOSO_RAW_DIR) not in sys.path:
    sys.path.insert(0, str(CONTOSO_RAW_DIR))


# (module_stem, dlt primary_key). primary_key is None when no natural PK
# is defined in the bruin asset metadata.
RAW_TABLES: list[tuple[str, object]] = [
    # Contoso V2 parquet sources
    ("customers", "customer_key"),
    ("dates", "date"),
    ("orders", "order_key"),
    ("sales", ["order_key", "line_number"]),
    ("products", "product_key"),
    ("stores", "store_key"),
    ("order_rows", ["order_key", "row_number"]),
    ("currency_exchange", ["date", "from_currency", "to_currency"]),
    # Synthetic (Faker) sources
    ("employees", "employee_key"),
    ("departments", "department_key"),
    ("payroll", None),
    ("campaigns", "campaign_key"),
    ("ad_spend_daily", None),
    ("campaign_attribution", None),
    ("gl_journal_entries", None),
    ("budgets", None),
    ("accounts_payable", "ap_key"),
    ("inventory_snapshots", None),
    ("shipments", "shipment_key"),
    ("support_tickets", "support_ticket_key"),
    ("sprint_tickets", "ticket_key"),
    ("deployments", None),
    ("job_postings", None),
]


def _load_materialize(module_stem: str):
    """Dynamically import `materialize` from contoso_raw/<stem>.py."""
    path = CONTOSO_RAW_DIR / f"{module_stem}.py"
    if not path.exists():
        raise FileNotFoundError(f"Expected {path} to exist")
    spec = importlib.util.spec_from_file_location(
        f"contoso_raw_{module_stem}", path
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not hasattr(module, "materialize"):
        raise AttributeError(f"{path} does not define materialize()")
    return module.materialize


def _make_resource(name: str, primary_key, materialize_fn):
    kwargs = {"name": name, "write_disposition": "replace"}
    if primary_key is not None:
        kwargs["primary_key"] = primary_key

    @dlt.resource(**kwargs)
    def _resource():
        logger.info("Materializing %s", name)
        df = materialize_fn()
        logger.info("%s produced %d rows", name, len(df))
        yield df

    _resource.__name__ = f"{name}_resource"
    return _resource


@dlt.source(name="contoso_raw")
def contoso_raw_source():
    for module_stem, pk in RAW_TABLES:
        materialize_fn = _load_materialize(module_stem)
        yield _make_resource(module_stem, pk, materialize_fn)


def run() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="contoso_dbt",
        destination="bigquery",
        dataset_name="contoso_dbt_raw",
        progress="log",
    )
    load_info = pipeline.run(contoso_raw_source())
    logger.info("dlt load complete: %s", load_info)


if __name__ == "__main__":
    run()
