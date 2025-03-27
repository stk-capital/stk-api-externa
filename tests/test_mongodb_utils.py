#!/usr/bin/env python
"""
MongoDB Utility Tests

This script tests the MongoDB utility functions for database name selection
and connection handling between production and development databases.
"""

import os
import sys
import unittest
from unittest.mock import patch
import logging

# Add the parent directory to the path to import the utility module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util.mongodb_utils import get_db_name, get_mongo_client, get_database, get_mongo_collection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TestMongoDBUtils(unittest.TestCase):
    """Test cases for MongoDB utility functions."""

    def setUp(self):
        """Set up test environment."""
        # Save original environment variable state
        self.original_env = os.environ.get("USE_DEV_MONGO_DB", None)

    def tearDown(self):
        """Restore original environment."""
        # Restore original environment variable
        if self.original_env is not None:
            os.environ["USE_DEV_MONGO_DB"] = self.original_env
        else:
            os.environ.pop("USE_DEV_MONGO_DB", None)

    def test_db_name_selection_default(self):
        """Test database name selection with default settings (no env var)."""
        # Remove environment variable if it exists
        if "USE_DEV_MONGO_DB" in os.environ:
            del os.environ["USE_DEV_MONGO_DB"]
        
        # Test with default settings (should use production)
        db_name = get_db_name("alphasync_db")
        self.assertEqual(db_name, "alphasync_db")

    def test_db_name_selection_production(self):
        """Test database name selection with USE_DEV_MONGO_DB=False."""
        # Set environment variable to use production
        os.environ["USE_DEV_MONGO_DB"] = "False"
        
        # Test with explicit production setting
        db_name = get_db_name("alphasync_db")
        self.assertEqual(db_name, "alphasync_db")

    def test_db_name_selection_development(self):
        """Test database name selection with USE_DEV_MONGO_DB=True."""
        # Set environment variable to use development
        os.environ["USE_DEV_MONGO_DB"] = "True"
        
        # Test with development setting
        db_name = get_db_name("alphasync_db")
        self.assertEqual(db_name, "alphasync_db_dev")
        
        # Test with STKFeed
        db_name = get_db_name("STKFeed")
        self.assertEqual(db_name, "STKFeed_dev")

    def test_get_database(self):
        """Test get_database function with different settings."""
        # Test with production setting
        os.environ["USE_DEV_MONGO_DB"] = "False"
        db = get_database("alphasync_db")
        self.assertEqual(db.name, "alphasync_db")
        
        # Test with development setting
        os.environ["USE_DEV_MONGO_DB"] = "True"
        db = get_database("alphasync_db")
        self.assertEqual(db.name, "alphasync_db_dev")

    def test_get_mongo_collection(self):
        """Test get_mongo_collection function with different settings."""
        # Test with production setting
        os.environ["USE_DEV_MONGO_DB"] = "False"
        collection = get_mongo_collection("chunks", "alphasync_db")
        self.assertEqual(collection.database.name, "alphasync_db")
        self.assertEqual(collection.name, "chunks")
        
        # Test with development setting
        os.environ["USE_DEV_MONGO_DB"] = "True"
        collection = get_mongo_collection("chunks", "alphasync_db")
        self.assertEqual(collection.database.name, "alphasync_db_dev")
        self.assertEqual(collection.name, "chunks")


if __name__ == "__main__":
    unittest.main() 