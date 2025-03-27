#!/usr/bin/env python
"""
MongoDB Database Structure Cloning Script

This script clones the structure (collections and indexes) from the production
databases (alphasync_db and STKFeed) to their development counterparts
(alphasync_db_dev and STKFeed_dev) without copying any data.

The script performs the following operations:
1. Creates empty collections with the same structure
2. Recreates all standard indexes (non-vector)
3. Provides guidance for recreating vector search indexes
4. Verifies that indexes have been created correctly
"""

import logging
import time
import os
import sys
import argparse
from pymongo import MongoClient
from pymongo.errors import OperationFailure, DuplicateKeyError

# Add the project root to the path to import the utility module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util.mongodb_utils import get_mongo_client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def clone_collections(source_db_name, target_db_name):
    """
    Clone collections from source to target database without data.
    
    Args:
        source_db_name (str): Name of the source database
        target_db_name (str): Name of the target database
    """
    client = get_mongo_client()
    source_db = client[source_db_name]
    target_db = client[target_db_name]
    
    logger.info(f"Cloning collections from {source_db_name} to {target_db_name}...")
    
    # Get existing collections
    source_collections = source_db.list_collection_names()
    target_collections = target_db.list_collection_names()
    
    # Create collections in the target database
    for collection_name in source_collections:
        # Skip system collections
        if collection_name.startswith("system."):
            continue
            
        if collection_name not in target_collections:
            logger.info(f"Creating collection {collection_name} in {target_db_name}")
            try:
                target_db.create_collection(collection_name)
            except Exception as e:
                logger.error(f"Error creating collection {collection_name}: {e}")
        else:
            logger.info(f"Collection {collection_name} already exists in {target_db_name}")
    
    logger.info(f"Successfully cloned collections from {source_db_name} to {target_db_name}")


def clone_standard_indexes(source_db_name, target_db_name):
    """
    Clone all standard indexes from source to target database.
    
    Args:
        source_db_name (str): Name of the source database
        target_db_name (str): Name of the target database
    """
    client = get_mongo_client()
    source_db = client[source_db_name]
    target_db = client[target_db_name]
    
    logger.info(f"Cloning indexes from {source_db_name} to {target_db_name}...")
    
    collection_count = 0
    index_count = 0
    error_count = 0
    
    for collection_name in source_db.list_collection_names():
        # Skip system collections
        if collection_name.startswith("system."):
            continue
            
        source_collection = source_db[collection_name]
        target_collection = target_db[collection_name]
        
        collection_count += 1
        logger.info(f"Processing indexes for collection: {collection_name}")
        
        for index in source_collection.list_indexes():
            # Skip _id_ index and vector indexes
            if index["name"] == "_id_" or "vector" in index.get("name", "").lower():
                continue
                
            # Extract index key and options
            keys = list(index["key"].items())
            
            # Extract index options (exclude metadata fields)
            options = {k: v for k, v in index.items() 
                    if k not in ["v", "key", "ns"]}
                    
            try:
                logger.info(f"  Creating index {index['name']} with keys {keys}")
                start_time = time.time()
                
                # Create index with background=True to minimize impact on running operations
                options["background"] = True
                target_collection.create_index(keys, **options)
                
                end_time = time.time()
                logger.info(f"  Created index {index['name']} in {end_time - start_time:.2f}s")
                index_count += 1
                
            except DuplicateKeyError:
                logger.info(f"  Index {index['name']} already exists, skipping")
            except OperationFailure as e:
                logger.error(f"  Error creating index {index['name']}: {e}")
                error_count += 1
            except Exception as e:
                logger.error(f"  Unexpected error creating index {index['name']}: {e}")
                error_count += 1
    
    logger.info(f"Index cloning summary: {collection_count} collections processed, "
                f"{index_count} indexes created, {error_count} errors")


def print_vector_index_instructions():
    """Print instructions for creating vector search indexes in Atlas."""
    logger.info("\n" + "=" * 80)
    logger.info("VECTOR SEARCH INDEX CREATION INSTRUCTIONS")
    logger.info("=" * 80)
    logger.info("Vector search indexes must be created manually in MongoDB Atlas UI:")
    logger.info("1. Log in to MongoDB Atlas: https://cloud.mongodb.com")
    logger.info("2. Navigate to your cluster > Database > Atlas Search")
    logger.info("3. Create the following vector indexes on the development databases:")
    logger.info("\nFor alphasync_db_dev:")
    logger.info("   a. Index name: vector_index_loop_events")
    logger.info("      - Collection: events")
    logger.info("      - Field mapped: embedding")
    logger.info("      - Dimensions: 1536")
    logger.info("      - Similarity: cosine")
    
    logger.info("   b. Index name: vector_index_loop_infos")
    logger.info("      - Collection: infos")
    logger.info("      - Field mapped: embedding")
    logger.info("      - Dimensions: 1536")
    logger.info("      - Similarity: cosine")
    
    logger.info("   c. Index name: vector_index")
    logger.info("      - Collection: companies")
    logger.info("      - Field mapped: embedding")
    logger.info("      - Dimensions: 1536")
    logger.info("      - Similarity: cosine")
    
    logger.info("   d. Index name: vector_index_loop_sources")
    logger.info("      - Collection: sources")
    logger.info("      - Field mapped: embedding")
    logger.info("      - Dimensions: 1536")
    logger.info("      - Similarity: cosine")
    
    logger.info("\n4. Wait for the indexes to be created (this may take several minutes)")
    logger.info("=" * 80)


def verify_vector_indexes(db_name):
    """
    Verify vector indexes exist and function correctly.
    
    Args:
        db_name (str): Name of the database to verify
    
    Returns:
        bool: True if all vector indexes are working, False otherwise
    """
    client = get_mongo_client()
    db = client[db_name]
    vector_indexes = {
        "events": ["vector_index_loop_events"],
        "infos": ["vector_index_loop_infos"],
        "companies": ["vector_index"],
        "sources": ["vector_index_loop_sources"]
    }
    
    logger.info(f"Verifying vector indexes in {db_name}...")
    all_indexes_working = True
    
    for collection_name, indexes in vector_indexes.items():
        # Skip if collection doesn't exist
        if collection_name not in db.list_collection_names():
            logger.warning(f"Collection {collection_name} does not exist in {db_name}, skipping vector index check")
            continue
            
        for index_name in indexes:
            try:
                # Use a non-zero vector to avoid cosine similarity errors
                test_vector = [0.1] * 1536
                result = list(db[collection_name].aggregate([
                    {
                        "$vectorSearch": {
                            "index": index_name,
                            "path": "embedding",
                            "queryVector": test_vector,
                            "numCandidates": 1,
                            "limit": 1
                        }
                    }
                ], maxTimeMS=5000))
                logger.info(f"Vector index {index_name} on {collection_name} is working")
            except Exception as e:
                logger.error(f"Vector index {index_name} on {collection_name} check failed: {e}")
                all_indexes_working = False
    
    return all_indexes_working


def main():
    """Main function to execute the database cloning process."""
    parser = argparse.ArgumentParser(description="Clone MongoDB database structure without data")
    parser.add_argument("--check-vector-indexes", action="store_true", 
                        help="Check if vector indexes are working properly")
    parser.add_argument("--skip-collections", action="store_true", 
                        help="Skip collection creation")
    parser.add_argument("--skip-indexes", action="store_true", 
                        help="Skip index creation")
    args = parser.parse_args()
    
    # Define source and target databases
    db_pairs = [
        ("alphasync_db", "alphasync_db_dev"),
        ("STKFeed", "STKFeed_dev")
    ]

    try:
        # Process each database pair
        for source_db, target_db in db_pairs:
            logger.info(f"Processing database pair: {source_db} -> {target_db}")
            
            # Clone collections
            if not args.skip_collections:
                clone_collections(source_db, target_db)
            else:
                logger.info("Skipping collection creation")
            
            # Clone standard indexes
            if not args.skip_indexes:
                clone_standard_indexes(source_db, target_db)
            else:
                logger.info("Skipping index creation")
            
        # Print instructions for vector index creation
        print_vector_index_instructions()
        
        # Verify vector indexes if requested
        if args.check_vector_indexes:
            for _, target_db in db_pairs:
                if verify_vector_indexes(target_db):
                    logger.info(f"All vector indexes in {target_db} are working properly")
                else:
                    logger.warning(f"Some vector indexes in {target_db} are not working properly")
        
        logger.info("Database structure cloning completed successfully")
        
    except Exception as e:
        logger.error(f"An error occurred during database cloning: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main()) 