from datetime import datetime
from loguru import logger

from src.utils.db import get_collection
from src.utils.logging_config import configure_logging


def clean_trips() -> None:
    configure_logging()

    raw = get_collection("raw_trips")
    clean = get_collection("clean_trips")

    logger.info("Starting cleaning process...")
    logger.info("Fetching documents from raw_trips...")

    cursor = raw.find({})

    inserted = 0
    invalid = 0
    batch = []
    batch_size = 10_000

    try:
        for doc in cursor:
            # ---- REQUIRED FIELDS FROM RAW ----
            vendor = doc.get("VendorID", None)
            pickup = doc.get("tpep_pickup_datetime", None)
            dropoff = doc.get("tpep_dropoff_datetime", None)

            # Must have VendorID + pickup/dropoff datetimes
            if vendor is None or pickup is None or dropoff is None:
                invalid += 1
                continue

            # Ensure datetimes (they already are in your raw example)
            if not isinstance(pickup, datetime) or not isinstance(dropoff, datetime):
                invalid += 1
                continue

            # Basic sanity
            if dropoff <= pickup:
                invalid += 1
                continue

            cleaned = {
                "VendorID": int(vendor),

                # rename to your clean-layer naming
                "pickup_datetime": pickup,
                "dropoff_datetime": dropoff,

                # keep these if present; otherwise default
                "passenger_count": doc.get("passenger_count"),
                "trip_distance": doc.get("trip_distance"),
                "fare_amount": doc.get("fare_amount"),
                "tip_amount": doc.get("tip_amount"),
                "total_amount": doc.get("total_amount"),
                "payment_type": str(doc.get("payment_type")) if doc.get("payment_type") is not None else None,

                # TLC parquet often uses these exact raw names; map them if present
                "pu_location_id": doc.get("PULocationID"),
                "do_location_id": doc.get("DOLocationID"),
            }

            batch.append(cleaned)

            if len(batch) >= batch_size:
                clean.insert_many(batch, ordered=False)
                inserted += len(batch)
                batch.clear()

                if inserted % 500_000 == 0:
                    logger.info(f"Inserted {inserted} cleaned docs so far...")

        # flush remainder
        if batch:
            clean.insert_many(batch, ordered=False)
            inserted += len(batch)

    finally:
        cursor.close()

    logger.info(f"Cleaning complete. Raw: {raw.estimated_document_count()}, Clean: {inserted}, Invalid: {invalid}")


if __name__ == "__main__":
    clean_trips()
