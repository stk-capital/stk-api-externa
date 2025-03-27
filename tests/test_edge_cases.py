#!/usr/bin/env python
"""
Edge Case Tests for MongoDB Development Databases

This script tests how the application handles edge cases such as:
- Missing databases
- Missing collections
- Missing indexes
- Vector search failures
"""

import os
import sys
import unittest
import logging
from unittest.mock import patch, MagicMock
import pymongo
from pymongo.errors import OperationFailure

# Add the parent directory to the path to import the utility module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util.mongodb_utils import get_mongo_client, get_db_name, get_mongo_collection
from email_processor import get_embedding, find_similar_info_vector_search

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TestEdgeCases(unittest.TestCase):
    """Test edge cases for MongoDB development databases."""

    def setUp(self):
        """Set up test environment."""
        # Save original environment variable state
        self.original_env = os.environ.get("USE_DEV_MONGO_DB", None)
        # Force development mode
        os.environ["USE_DEV_MONGO_DB"] = "True"
        # Get a MongoDB client
        self.client = get_mongo_client()
        # Define database names
        self.db_name = get_db_name("alphasync_db")

    def tearDown(self):
        """Restore original environment."""
        # Close the MongoDB client
        self.client.close()
        # Restore original environment variable
        if self.original_env is not None:
            os.environ["USE_DEV_MONGO_DB"] = self.original_env
        else:
            os.environ.pop("USE_DEV_MONGO_DB", None)

    def test_missing_database_handling(self):
        """Test how the application handles a missing database."""
        # Mock the get_mongo_client function to return a client with no databases
        with patch('util.mongodb_utils.get_mongo_client') as mock_client:
            # Set up the mock to raise an exception when accessing the database
            mock_db = MagicMock()
            mock_db.__getitem__.side_effect = pymongo.errors.ServerSelectionTimeoutError(
                "No servers found yet"
            )
            mock_client.return_value = mock_db
            
            # Test accessing a non-existent database
            with self.assertRaises(pymongo.errors.ServerSelectionTimeoutError):
                get_mongo_collection("chunks", "nonexistent_db")

    def test_missing_collection_handling(self):
        """Test how the application handles a missing collection."""
        # Test accessing a potentially non-existent collection
        # This should not raise an exception as MongoDB creates collections on demand
        try:
            collection = get_mongo_collection("nonexistent_collection", "alphasync_db")
            # Insert a document to ensure the collection is created
            collection.insert_one({"test": True})
            # Delete the document to clean up
            collection.delete_one({"test": True})
        except Exception as e:
            self.fail(f"Accessing a non-existent collection raised: {e}")

    def test_missing_index_handling(self):
        """Test how the application handles queries on collections with missing indexes."""
        # Create a test collection without indexes
        db = self.client[self.db_name]
        test_collection_name = "test_collection_no_indexes"
        
        # Ensure clean state
        if test_collection_name in db.list_collection_names():
            db[test_collection_name].drop()
        
        db.create_collection(test_collection_name)
        
        # Insert some test documents
        test_docs = [
            {"field1": i, "field2": f"value{i}", "timestamp": i * 1000}
            for i in range(10)
        ]
        db[test_collection_name].insert_many(test_docs)
        
        # Execute a query that would normally use an index
        # This should still work, just be less efficient
        result = list(db[test_collection_name].find({"field1": 5}))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["field2"], "value5")
        
        # Clean up
        db[test_collection_name].drop()

    @patch('email_processor.get_embedding')
    def test_vector_search_fallback(self, mock_get_embedding):
        """Test fallback behavior when vector search fails."""
        # Mock the embedding function
        mock_get_embedding.return_value = [0.1] * 1536  # Create a dummy embedding
        
        # Create a mock chunk
        mock_chunk = MagicMock()
        mock_chunk.embedding = [0.1] * 1536
        
        # Create a mock infos collection that raises an error on aggregation
        mock_infos_collection = MagicMock()
        mock_infos_collection.aggregate.side_effect = OperationFailure(
            "The $vectorSearch stage requires that the parameter 'index' must be a valid Atlas Search vector index name"
        )
        
        # Test the function's error handling
        result = find_similar_info_vector_search(mock_chunk, mock_infos_collection)
        
        # Verify the function handled the error gracefully
        self.assertIsNone(result)  # Should return None when vector search fails
        mock_infos_collection.aggregate.assert_called_once()  # Should have attempted the search

    def test_database_switching(self):
        """Test switching between production and development databases."""
        # Get a reference to a collection with dev mode
        os.environ["USE_DEV_MONGO_DB"] = "True"
        dev_collection = get_mongo_collection("emails", "alphasync_db")
        self.assertEqual(dev_collection.database.name, "alphasync_db_dev")
        
        # Switch to production mode
        os.environ["USE_DEV_MONGO_DB"] = "False"
        prod_collection = get_mongo_collection("emails", "alphasync_db")
        self.assertEqual(prod_collection.database.name, "alphasync_db")
        
        # They should be different database objects
        self.assertNotEqual(dev_collection.database.name, prod_collection.database.name)

    def test_case_insensitive_env_var(self):
        """Test that the USE_DEV_MONGO_DB env var is case insensitive."""
        # Test with lowercase "true"
        os.environ["USE_DEV_MONGO_DB"] = "true"
        collection = get_mongo_collection("emails", "alphasync_db")
        self.assertEqual(collection.database.name, "alphasync_db_dev")
        
        # Test with mixed case "TrUe"
        os.environ["USE_DEV_MONGO_DB"] = "TrUe"
        collection = get_mongo_collection("emails", "alphasync_db")
        self.assertEqual(collection.database.name, "alphasync_db_dev")
        
        # Test with uppercase "FALSE"
        os.environ["USE_DEV_MONGO_DB"] = "FALSE"
        collection = get_mongo_collection("emails", "alphasync_db")
        self.assertEqual(collection.database.name, "alphasync_db")


if __name__ == "__main__":
    unittest.main() 