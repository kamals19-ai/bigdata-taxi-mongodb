from datetime import datetime, timezone
from loguru import logger

from src.utils.db import get_collection
from src.utils.logging_config import configure_logging


def aggregate_trips() -> None:
    configure_logging()

    clean = get_collection("clean_trips")
    agg = get_collection("agg_trips")

    start_dt = datetime(2025, 5, 1, tzinfo=timezone.utc)
    end_dt = datetime(2025, 8, 1, tzinfo=timezone.utc)

    logger.info(f"Aggregating ONLY pickup window: {start_dt} -> {end_dt}")
    logger.info("Source collection: clean_trips")
    logger.info("Target collection: agg_trips")

    pipeline = [
        # Filter to date window + valid VendorID
        {
            "$match": {
                "pickup_datetime": {"$gte": start_dt, "$lt": end_dt},
                "VendorID": {"$exists": True, "$ne": None},
            }
        },
        # Add month key like "2025-06"
        {
            "$addFields": {
                "month": {
                    "$dateToString": {
                        "format": "%Y-%m",
                        "date": "$pickup_datetime",
                    }
                }
            }
        },
        # Aggregate metrics by month + VendorID
        {
            "$group": {
                "_id": {"month": "$month", "VendorID": "$VendorID"},
                "trip_count": {"$sum": 1},
                "avg_fare": {"$avg": "$fare_amount"},
                "avg_tip": {"$avg": "$tip_amount"},
                "total_revenue": {"$sum": "$total_amount"},
            }
        },
        # Final output shape
        {
            "$project": {
                "_id": 0,
                "month": "$_id.month",
                "VendorID": "$_id.VendorID",
                "trip_count": 1,
                "avg_fare": {"$round": ["$avg_fare", 2]},
                "avg_tip": {"$round": ["$avg_tip", 2]},
                "total_revenue": {"$round": ["$total_revenue", 2]},
            }
        },
        # Write results to agg_trips (replaces collection each run)
        {"$out": "agg_trips"},
    ]

    logger.info("Running aggregation pipeline...")
    clean.aggregate(pipeline, allowDiskUse=True)
    logger.info("Aggregation complete!")
    logger.info(f"agg_trips docs now = {agg.count_documents({})}")


if __name__ == "__main__":
    aggregate_trips()
