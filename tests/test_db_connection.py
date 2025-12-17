from src.utils.db import get_collection


def test_db_connection():
    """
    Verifies that a MongoDB collection object can be created.
    """
    collection = get_collection("agg_trips")
    assert collection.name == "agg_trips"
