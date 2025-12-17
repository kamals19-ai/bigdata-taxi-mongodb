import os
import polars as pl
from loguru import logger

from src.utils.db import get_collection
from src.utils.logging_config import configure_logging


def load_raw() -> None:
    """Load raw NYC taxi Parquet file(s) into MongoDB."""
    configure_logging()

    paths_env = os.getenv("TAXI_CSV_PATH")  # (name kept for compatibility)
    raw_collection_name = os.getenv("RAW_COLLECTION", "raw_trips")

    if not paths_env:
        raise RuntimeError("TAXI_CSV_PATH is not set in .env")

    # Support single path OR comma-separated list of paths
    parquet_paths = [p.strip() for p in paths_env.split(",") if p.strip()]

    # Optional cap on total rows inserted across ALL files
    max_rows_env = os.getenv("MAX_ROWS")
    max_rows = int(max_rows_env) if max_rows_env else None

    coll = get_collection(raw_collection_name)

    batch_size = 10_000
    inserted_total = 0

    for parquet_path in parquet_paths:
        logger.info(f"Reading Parquet file from: {parquet_path}")
        df = pl.read_parquet(parquet_path)
        logger.info(f"Loaded {df.height} rows and {df.width} columns")

        # Apply global cap across all files (if MAX_ROWS is set)
        if max_rows is not None:
            remaining = max_rows - inserted_total
            if remaining <= 0:
                logger.info(f"MAX_ROWS reached ({max_rows}). Stopping ingestion.")
                break
            if df.height > remaining:
                logger.info(f"Truncating this file to {remaining} rows to honor MAX_ROWS={max_rows}")
                df = df.head(remaining)

        total_rows = df.height
        logger.info(f"Inserting {total_rows} rows into collection '{raw_collection_name}'...")

        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            batch = df[start:end]
            records = batch.to_dicts()
            if records:
                coll.insert_many(records)
                inserted_total += len(records)
                logger.info(f"Inserted rows {start}–{end} of {total_rows} (global inserted: {inserted_total})")

    logger.info("Raw ingestion complete.")


if __name__ == "__main__":
    load_raw()
