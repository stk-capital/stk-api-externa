#!/usr/bin/env python
"""
MongoDB Database Cloning Tests

This script tests the functionality of the database cloning process,
verifying that collections and indexes are correctly created in the
development databases.
"""

import os
import sys
import unittest
import logging

# Add the parent directory to the path to import the utility module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util.mongodb_utils import get_mongo_client, get_db_name
from scripts.clone_db_structure import clone_collections, clone_standard_indexes

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TestDBClone(unittest.TestCase):
    """Test cases for database cloning functionality."""

    def setUp(self):
        """Set up test environment."""
        # Save original environment variable state
        self.original_env = os.environ.get("USE_DEV_MONGO_DB", None)
        # Force development mode
        os.environ["USE_DEV_MONGO_DB"] = "True"
        # Get a MongoDB client
        self.client = get_mongo_client()
        # Define database names
        self.source_db_name = "alphasync_db"
        self.target_db_name = get_db_name(self.source_db_name)

    def tearDown(self):
        """Restore original environment."""
        # Close the MongoDB client
        self.client.close()
        # Restore original environment variable
        if self.original_env is not None:
            os.environ["USE_DEV_MONGO_DB"] = self.original_env
        else:
            os.environ.pop("USE_DEV_MONGO_DB", None)

    def test_collection_existence(self):
        """Test if collections exist in both source and target databases."""
        # Clone collections if not already done
        clone_collections(self.source_db_name, self.target_db_name)
        
        # Get list of collections in source and target databases
        source_collections = self.client[self.source_db_name].list_collection_names()
        target_collections = self.client[self.target_db_name].list_collection_names()
        
        # Filter out system collections
        source_collections = [c for c in source_collections if not c.startswith("system.")]
        target_collections = [c for c in target_collections if not c.startswith("system.")]
        
        # Check if all non-system collections from source exist in target
        for collection in source_collections:
            with self.subTest(collection=collection):
                self.assertIn(collection, target_collections, 
                             f"Collection {collection} does not exist in {self.target_db_name}")
        
        # Log the collections found
        logger.info(f"Found {len(source_collections)} collections in source database")
        logger.info(f"Found {len(target_collections)} collections in target database")

    def test_index_cloning(self):
        """Test if indexes are correctly cloned from source to target database."""
        # Clone collections and indexes
        clone_collections(self.source_db_name, self.target_db_name)
        clone_standard_indexes(self.source_db_name, self.target_db_name)
        
        # Get list of collections in source database (excluding system collections)
        source_collections = [c for c in self.client[self.source_db_name].list_collection_names() 
                              if not c.startswith("system.")]
        
        # Check if indexes exist in target collections
        index_count = 0
        for collection_name in source_collections:
            source_collection = self.client[self.source_db_name][collection_name]
            target_collection = self.client[self.target_db_name][collection_name]
            
            # Get indexes from both collections
            source_indexes = list(source_collection.list_indexes())
            target_indexes = list(target_collection.list_indexes())
            
            # Filter out vector indexes and _id_ index
            source_indexes = [idx for idx in source_indexes 
                              if idx["name"] != "_id_" and "vector" not in idx.get("name", "").lower()]
            target_indexes = [idx for idx in target_indexes 
                              if idx["name"] != "_id_" and "vector" not in idx.get("name", "").lower()]
            
            # Log number of indexes
            logger.info(f"Collection {collection_name}: {len(source_indexes)} source indexes, "
                       f"{len(target_indexes)} target indexes")
            
            # Check if index counts match
            with self.subTest(collection=collection_name):
                # Check if all standard indexes from source exist in target by name
                source_index_names = {idx["name"] for idx in source_indexes}
                target_index_names = {idx["name"] for idx in target_indexes}
                
                for idx_name in source_index_names:
                    with self.subTest(index=idx_name):
                        self.assertIn(idx_name, target_index_names,
                                     f"Index {idx_name} not found in {collection_name}")
                        index_count += 1
        
        logger.info(f"Verified {index_count} indexes across all collections")

    def test_critical_indexes(self):
        """Test if critical indexes are properly created."""
        # Clone collections and indexes
        clone_collections(self.source_db_name, self.target_db_name)
        clone_standard_indexes(self.source_db_name, self.target_db_name)
        
        # Check for critical indexes
        db = self.client[self.target_db_name]
        
        # Critical indexes to check
        critical_indexes = {
            "chunks": ["idx_chunks_has_events", "idx_chunks_has_events_was_processed"],
            "events": ["date_1", "companies_ids_1"],
            "emails": ["idx_emails_message_id"]
        }
        
        # Check each critical index
        for collection_name, indexes in critical_indexes.items():
            if collection_name not in db.list_collection_names():
                logger.warning(f"Collection {collection_name} not found in {self.target_db_name}")
                continue
                
            actual_indexes = [idx["name"] for idx in db[collection_name].list_indexes()]
            
            for idx_name in indexes:
                with self.subTest(collection=collection_name, index=idx_name):
                    self.assertIn(idx_name, actual_indexes,
                                 f"Critical index {idx_name} not found in {collection_name}")


if __name__ == "__main__":
    unittest.main() 