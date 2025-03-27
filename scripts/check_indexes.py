from pymongo import MongoClient
import os
import sys

# Add the project root to the path to import the utility module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util.mongodb_utils import get_mongo_client, get_db_name

# Connect to MongoDB using the utility function
client = get_mongo_client()

# Get the correct database names (respecting development mode)
alphasync_db_name = get_db_name("alphasync_db")
stkfeed_db_name = get_db_name("STKFeed")

# Print indexes for alphasync_db
print(f"\n{alphasync_db_name} indexes:")
for coll_name in client[alphasync_db_name].list_collection_names():
    print(f"\nCollection: {coll_name}")
    for idx in client[alphasync_db_name][coll_name].list_indexes():
        print(f"  {idx['name']}: {idx['key']}")

# Print indexes for STKFeed
print(f"\n{stkfeed_db_name} indexes:")
for coll_name in client[stkfeed_db_name].list_collection_names():
    print(f"\nCollection: {coll_name}")
    for idx in client[stkfeed_db_name][coll_name].list_indexes():
        print(f"  {idx['name']}: {idx['key']}")
