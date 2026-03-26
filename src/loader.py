# src/loader.py

import os
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from datetime import datetime

try:
    from logger import get_logger
except ModuleNotFoundError:
    from src.logger import get_logger

logger = get_logger(__name__)

# ── config ───────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(BASE_DIR, "gcp-key.json")

PROJECT_ID = "real-estate-intel-491208"
DATASET_ID = "real_estate"
TABLE_ID = "properties"
FULL_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# ── schema ───────────────────────────────────────────────────────────

SCHEMA = [
    bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("seller_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("price", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("size", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("unit", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("area", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("bedrooms", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("bathrooms", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("property_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("property_sub_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("purpose", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("created_at", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("size_sqft", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("scraped_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("price_label", "STRING", mode="NULLABLE"),
]


# ── helpers ──────────────────────────────────────────────────────────


def get_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


def create_table_if_not_exists(client: bigquery.Client) -> None:
    table_ref = bigquery.Table(FULL_TABLE, schema=SCHEMA)
    table_ref.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="scraped_date",
    )
    try:
        client.get_table(FULL_TABLE)
        logger.info(f"Table {FULL_TABLE} already exists")
    except Exception:
        client.create_table(table_ref)
        logger.info(f"Table {FULL_TABLE} created with date partitioning")


# ── main load function ───────────────────────────────────────────────


def load(df: pd.DataFrame) -> None:
    logger.info("================================================")
    logger.info(f"LOADER START — {len(df)} properties")
    logger.info("================================================")

    if df.empty:
        logger.warning("Empty DataFrame — skipping load")
        return

    client = get_client()
    create_table_if_not_exists(client)

    # ensure date columns are strings for BigQuery
    df["scraped_date"] = df["scraped_date"].astype(str)
    df["created_at"] = df["created_at"].astype(str)

    pandas_gbq.to_gbq(
        df,
        destination_table=f"{DATASET_ID}.{TABLE_ID}",
        project_id=PROJECT_ID,
        if_exists="append",
    )

    logger.info(f"Successfully loaded {len(df)} rows into {FULL_TABLE}")
    logger.info("================================================")
    logger.info(f"LOADER END — {datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}")
    logger.info("================================================")


if __name__ == "__main__":
    try:
        from extract import extract_all
        from transform import transform_data
        from quality import check_quality
    except ModuleNotFoundError:
        from src.extract import extract_all
        from src.transform import transform_data
        from src.quality import check_quality

    raw = extract_all(pages=5)
    df = transform_data(raw)
    report = check_quality(df)

    print(f"Total rows: {report['total_rows']}")
    print(f"Total issues: {report['total_issues']}")

    load(df)
    print(f"\nDone. {len(df)} properties loaded to BigQuery.")
