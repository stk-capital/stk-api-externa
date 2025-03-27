from pymongo import MongoClient
import os
import json
import sys

# Add the project root to the path to import the utility module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util.mongodb_utils import get_mongo_client, get_db_name

# Connect to MongoDB using utility function
client = get_mongo_client()

# Get the correct database names (respecting development mode)
alphasync_db_name = get_db_name("alphasync_db")

# Check for vector search index usage in the databases
vector_indexes = {
    alphasync_db_name: {
        "events": ["vector_index_loop_events"],
        "infos": ["vector_index_loop_infos"],
        "companies": ["vector_index"],
        "sources": ["vector_index_loop_sources"]
    }
}

print("\nChecking vector search indexes:")
for db_name, collections in vector_indexes.items():
    print(f"\nDatabase: {db_name}")
    db = client[db_name]
    
    for collection_name, index_names in collections.items():
        print(f"  Collection: {collection_name}")
        
        # Try running a dummy vector search query to see if the index exists
        for index_name in index_names:
            try:
                # Create a dummy vector of the right size
                dummy_vector = [0.0] * 1536  # Common embedding size for OpenAI embeddings
                
                # Try a simple query to check if index exists
                result = list(db[collection_name].aggregate([
                    {
                        "$vectorSearch": {
                            "index": index_name,
                            "path": "embedding",
                            "queryVector": dummy_vector,
                            "numCandidates": 1,
                            "limit": 1
                        }
                    }
                ], maxTimeMS=1000))  # Set a short timeout
                
                print(f"    ✓ Vector index '{index_name}' exists and is queryable")
            except Exception as e:
                print(f"    ✗ Error with vector index '{index_name}': {str(e)}") 