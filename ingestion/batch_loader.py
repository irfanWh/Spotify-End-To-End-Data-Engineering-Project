"""
Batch Loader — Loads Kaggle CSV datasets into PostgreSQL raw schema.

Reads CSV files from data_sources/raw/ and bulk-loads them into the
raw schema tables in PostgreSQL. Idempotent: truncates target tables
before each load to prevent duplicates on re-runs.
"""

import os
import sys
import time
import logging

import pandas as pd
from sqlalchemy import create_engine, text

# ──────────────── Configuration ────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

POSTGRES_USER = os.environ.get("POSTGRES_USER", "spotify_user")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "spotify_pass")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "spotify_platform")

DATABASE_URL = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Base path for raw data files
DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data_sources/raw")

# Dataset-to-table mapping
DATASETS = [
    {
        "file": "spotify_data clean.csv",
        "table": "raw.global",
        "encoding": "utf-8",
        "chunk_size": 10_000,
    },
    {
        "file": "Most Streamed Spotify Songs 2024.csv",
        "table": "raw.streams_2024",
        "encoding": "latin-1",
        "chunk_size": 10_000,
    },
    {
        "file": "tracks_features.csv",
        "table": "raw.tracks_1m",
        "encoding": "utf-8",
        "chunk_size": 100_000,
        "limit": 300_000,
    },
    {
        "file": "tracks.csv",
        "table": "raw.tracks_historical",
        "encoding": "utf-8",
        "chunk_size": 100_000,
        "limit": 100_000,
    },
    {
        "file": "artists.csv",
        "table": "raw.artists",
        "encoding": "latin-1",
        "chunk_size": 50_000,
    },
]


def get_engine():
    """
    Create and return a SQLAlchemy engine connected to PostgreSQL.

    Returns:
        sqlalchemy.Engine: Database engine instance.
    """
    return create_engine(DATABASE_URL, pool_pre_ping=True)


def wait_for_postgres(engine, max_retries=30, delay=2):
    """
    Wait for PostgreSQL to become available.

    Args:
        engine: SQLAlchemy engine instance.
        max_retries: Maximum number of connection attempts.
        delay: Seconds to wait between retries.

    Raises:
        ConnectionError: If Postgres is not available after max_retries.
    """
    for attempt in range(1, max_retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("PostgreSQL is ready.")
            return
        except Exception:
            logger.warning(
                "Waiting for PostgreSQL... attempt %d/%d", attempt, max_retries
            )
            time.sleep(delay)
    raise ConnectionError("PostgreSQL not available after %d retries." % max_retries)


def clean_column_names(df):
    """
    Standardise DataFrame column names to snake_case.

    Args:
        df: pandas DataFrame with raw column names.

    Returns:
        pandas.DataFrame: DataFrame with cleaned column names.
    """
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(r"[^a-z0-9_]", "_", regex=True)
        .str.replace(r"_+", "_", regex=True)
        .str.strip("_")
    )
    return df


def load_dataset(engine, dataset_config):
    """
    Load a single CSV file into a PostgreSQL table.

    Truncates the target table first, then reads the CSV in chunks
    and appends each chunk to the table.

    Args:
        engine: SQLAlchemy engine instance.
        dataset_config: Dictionary with keys: file, table, encoding, chunk_size.

    Returns:
        int: Total number of rows loaded.
    """
    file_path = os.path.join(DATA_DIR, dataset_config["file"])
    table_full = dataset_config["table"]
    schema, table_name = table_full.split(".")
    encoding = dataset_config["encoding"]
    chunk_size = dataset_config["chunk_size"]

    if not os.path.exists(file_path):
        logger.warning("File not found: %s — skipping.", file_path)
        return 0

    start_time = time.time()
    logger.info("Loading %s → %s", dataset_config["file"], table_full)

    # Truncate table for idempotency
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_full}"))
        conn.commit()
    logger.info("Truncated %s", table_full)

    total_rows = 0
    for chunk_num, chunk in enumerate(
        pd.read_csv(
            file_path,
            encoding=encoding,
            chunksize=chunk_size,
            low_memory=False,
        ),
        start=1,
    ):
        if "limit" in dataset_config and total_rows >= dataset_config["limit"]:
            logger.info("  Reached limit of %d rows. Stopping.", dataset_config["limit"])
            break
        
        chunk = clean_column_names(chunk)
        chunk.to_sql(
            name=table_name,
            schema=schema,
            con=engine,
            if_exists="append",
            index=False,
            method=None,
        )
        total_rows += len(chunk)
        logger.info(
            "  Chunk %d: %d rows loaded (total: %d)",
            chunk_num,
            len(chunk),
            total_rows,
        )

    elapsed = time.time() - start_time
    logger.info(
        "Finished %s: %d rows in %.1f seconds.", table_full, total_rows, elapsed
    )
    return total_rows


def main():
    """
    Main entry point: loads all configured CSV datasets into PostgreSQL.

    Returns:
        None
    """
    logger.info("=" * 60)
    logger.info("Spotify Batch Loader — Starting")
    logger.info("=" * 60)

    engine = get_engine()
    wait_for_postgres(engine)

    overall_start = time.time()
    summary = {}

    for ds in DATASETS:
        try:
            row_count = load_dataset(engine, ds)
            summary[ds["table"]] = row_count
        except Exception as exc:
            logger.error("Failed to load %s: %s", ds["file"], exc)
            summary[ds["table"]] = f"ERROR: {exc}"

    overall_elapsed = time.time() - overall_start

    logger.info("=" * 60)
    logger.info("LOAD SUMMARY")
    logger.info("-" * 60)
    for table, count in summary.items():
        logger.info("  %-30s : %s rows", table, count)
    logger.info("-" * 60)
    logger.info("Total elapsed: %.1f seconds", overall_elapsed)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
