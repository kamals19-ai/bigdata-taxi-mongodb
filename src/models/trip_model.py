from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class Trip(BaseModel):
    # Use aliases that match the TLC column names in the Parquet
    pickup_datetime: datetime = Field(alias="tpep_pickup_datetime")
    dropoff_datetime: datetime = Field(alias="tpep_dropoff_datetime")

    passenger_count: float
    trip_distance: float
    fare_amount: float
    tip_amount: float
    total_amount: float

    payment_type: Optional[str] = None
    pu_location_id: Optional[int] = Field(default=None, alias="PULocationID")
    do_location_id: Optional[int] = Field(default=None, alias="DOLocationID")

    @field_validator("payment_type", mode="before")
    @classmethod
    def normalize_payment_type(cls, v: object) -> str:
        if v is None:
            return "unknown"
        return str(v).strip().lower()

    @field_validator("passenger_count", "trip_distance", "fare_amount", "total_amount")
    @classmethod
    def non_negative(cls, v: float) -> float:
        # Filter out weird negatives / nonsense
        if v is None:
            raise ValueError("value cannot be None")
        if float(v) < 0:
            raise ValueError("value cannot be negative")
        return float(v)
