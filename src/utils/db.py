from typing import Any
import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

def get_client() -> MongoClient:
    uri = os.getenv("MONGO_URI")
    if not uri:
        raise RuntimeError("MONGO_URI is not set")
    return MongoClient(uri)

def get_collection(name: str):
    db_name = os.getenv("MONGO_DB_NAME", "bigdata_taxi")
    client = get_client()
    return client[db_name][name]
