"""
Aggregation pipeline for NYC Taxi Gold Layer (agg_trips)

Purpose:
- Aggregate cleaned taxi trips by month and VendorID
- Produce business-ready metrics for dashboard consumption
"""

AGG_TRIPS_PIPELINE = [
    {
        "$group": {
            "_id": {
                "month": {
                    "$dateToString": {
                        "format": "%Y-%m",
                        "date": "$pickup_datetime"
                    }
                },
                "VendorID": "$VendorID"
            },
            "trip_count": {"$sum": 1},
            "avg_fare": {"$avg": "$fare_amount"},
            "avg_tip": {"$avg": "$tip_amount"},
            "total_revenue": {
                "$sum": {
                    "$add": ["$fare_amount", "$tip_amount"]
                }
            }
        }
    },
    {
        "$project": {
            "_id": 0,
            "month": "$_id.month",
            "VendorID": "$_id.VendorID",
            "trip_count": 1,
            "avg_fare": {"$round": ["$avg_fare", 2]},
            "avg_tip": {"$round": ["$avg_tip", 2]},
            "total_revenue": {"$round": ["$total_revenue", 2]}
        }
    },
    {"$sort": {"month": 1, "VendorID": 1}}
]
