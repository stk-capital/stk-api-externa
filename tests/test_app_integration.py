#!/usr/bin/env python
"""
Application Integration Tests with Development Databases

This script tests the core application functionality with development
databases to ensure everything works correctly with the database
selection mechanism.
"""

import os
import sys
import unittest
import logging
from datetime import datetime

# Add the parent directory to the path to import the utility module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util.mongodb_utils import get_mongo_client, get_db_name, get_mongo_collection
from email_processor import Email, Chunk, get_embedding, process_full_pipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TestAppIntegration(unittest.TestCase):
    """Test application integration with development databases."""

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
        # Clear any existing test data
        self._clear_test_data()

    def tearDown(self):
        """Clean up test environment."""
        # Clear test data
        self._clear_test_data()
        # Close the MongoDB client
        self.client.close()
        # Restore original environment variable
        if self.original_env is not None:
            os.environ["USE_DEV_MONGO_DB"] = self.original_env
        else:
            os.environ.pop("USE_DEV_MONGO_DB", None)

    def _clear_test_data(self):
        """Clear any existing test data from previous runs."""
        db = self.client[self.db_name]
        
        # Remove test documents from each collection
        test_data_filter = {"_test_data": True}
        for collection_name in ["emails", "chunks", "infos", "events", "companies", "sources"]:
            if collection_name in db.list_collection_names():
                db[collection_name].delete_many(test_data_filter)

    def _create_test_email(self):
        """Create a test email for the integration test."""
        test_email = Email(
            message_id="test_message_id",
            from_address="test@example.com",
            subject="Test Email for Development Database",
            body="This is a test email containing company information about Apple Inc. and Microsoft Corp.",
            received_at=datetime.now(),
            was_processed=False,
            relevant=True,
            _test_data=True
        )
        
        # Insert the test email
        emails_collection = get_mongo_collection("emails", "alphasync_db")
        email_id = emails_collection.insert_one(test_email.dict(by_alias=True)).inserted_id
        
        logger.info(f"Created test email with ID: {email_id}")
        return str(email_id)

    def _create_test_chunk(self, email_id):
        """Create a test chunk for the integration test."""
        # Generate a test embedding
        test_embedding = get_embedding("Test chunk about Apple Inc.")
        
        test_chunk = Chunk(
            content="This is a test chunk containing information about Apple Inc.",
            summary="Information about Apple Inc.",
            subject="Test Email Subject",
            source="test_email",
            instrument_ids=["AAPL"],
            embedding=test_embedding,
            include=True,
            has_events=True,
            document_id=email_id,
            document_collection="emails",
            index=0,
            was_processed=False,
            was_processed_events=False,
            _test_data=True
        )
        
        # Insert the test chunk
        chunks_collection = get_mongo_collection("chunks", "alphasync_db")
        chunk_id = chunks_collection.insert_one(test_chunk.dict(by_alias=True)).inserted_id
        
        logger.info(f"Created test chunk with ID: {chunk_id}")
        return str(chunk_id)

    def _create_test_company(self):
        """Create a test company for the integration test."""
        # Generate a test embedding
        test_embedding = get_embedding("Apple Inc. is a technology company.")
        
        # Create test company document
        company_data = {
            "name": "Apple Inc.",
            "ticker": "AAPL",
            "public": True,
            "parent_company": "",
            "description": "Apple Inc. is an American multinational technology company.",
            "sector": "Technology",
            "embedding": test_embedding,
            "created_at": datetime.now(),
            "_test_data": True
        }
        
        # Insert the test company
        companies_collection = get_mongo_collection("companies", "alphasync_db")
        company_id = companies_collection.insert_one(company_data).inserted_id
        
        logger.info(f"Created test company with ID: {company_id}")
        return str(company_id)

    def test_database_selection(self):
        """Test that the application uses the development database when configured."""
        # Verify we're using the development database
        emails_collection = get_mongo_collection("emails", "alphasync_db")
        self.assertEqual(emails_collection.database.name, "alphasync_db_dev")

    def test_basic_crud_operations(self):
        """Test basic CRUD operations with development database."""
        # Create a test email
        email_id = self._create_test_email()
        
        # Retrieve the email and verify it exists
        emails_collection = get_mongo_collection("emails", "alphasync_db")
        test_email = emails_collection.find_one({"_id": email_id})
        
        self.assertIsNotNone(test_email)
        self.assertEqual(test_email["message_id"], "test_message_id")
        self.assertEqual(test_email["subject"], "Test Email for Development Database")
        
        # Update the email
        emails_collection.update_one(
            {"_id": email_id},
            {"$set": {"was_processed": True}}
        )
        
        # Verify the update worked
        updated_email = emails_collection.find_one({"_id": email_id})
        self.assertTrue(updated_email["was_processed"])
        
        # Delete the email
        emails_collection.delete_one({"_id": email_id})
        
        # Verify deletion
        deleted_email = emails_collection.find_one({"_id": email_id})
        self.assertIsNone(deleted_email)

    def test_chunk_creation_and_search(self):
        """Test creating chunks and performing vector search."""
        # Create a test email and chunk
        email_id = self._create_test_email()
        chunk_id = self._create_test_chunk(email_id)
        
        # Retrieve the chunk and verify it exists
        chunks_collection = get_mongo_collection("chunks", "alphasync_db")
        test_chunk = chunks_collection.find_one({"_id": chunk_id})
        
        self.assertIsNotNone(test_chunk)
        self.assertTrue(test_chunk["has_events"])
        self.assertEqual(test_chunk["document_id"], email_id)
        
        # Create a test company
        company_id = self._create_test_company()
        
        # Check if the company exists
        companies_collection = get_mongo_collection("companies", "alphasync_db")
        test_company = companies_collection.find_one({"_id": company_id})
        
        self.assertIsNotNone(test_company)
        self.assertEqual(test_company["ticker"], "AAPL")
        
        # Note: Full vector search test would require setting up vector indexes in the dev database
        # which we can't do automatically in this test. We're only testing basic operations here.

    def test_query_with_indexes(self):
        """Test queries that use indexes."""
        # Create several test emails with different dates
        emails_collection = get_mongo_collection("emails", "alphasync_db")
        
        # Create 5 test emails
        for i in range(5):
            test_email = Email(
                message_id=f"test_message_id_{i}",
                from_address="test@example.com",
                subject=f"Test Email {i}",
                body=f"This is test email {i}",
                received_at=datetime.now(),
                was_processed=False,
                relevant=True,
                _test_data=True
            )
            
            emails_collection.insert_one(test_email.dict(by_alias=True))
        
        # Query using an indexed field (message_id should be indexed)
        email = emails_collection.find_one({"message_id": "test_message_id_2"})
        self.assertIsNotNone(email)
        self.assertEqual(email["subject"], "Test Email 2")
        
        # Create test chunks with has_events
        chunks_collection = get_mongo_collection("chunks", "alphasync_db")
        
        # Create 3 chunks: 2 with has_events=True, 1 with has_events=False
        for i in range(3):
            has_events = i < 2  # First 2 have events
            
            test_chunk = Chunk(
                content=f"Test chunk {i}",
                summary=f"Summary {i}",
                subject=f"Subject {i}",
                source="test_source",
                instrument_ids=[],
                embedding=get_embedding(f"Test chunk {i}"),
                include=True,
                has_events=has_events,
                document_id=f"test_doc_{i}",
                document_collection="test_collection",
                index=i,
                was_processed=False,
                was_processed_events=False,
                _test_data=True
            )
            
            chunks_collection.insert_one(test_chunk.dict(by_alias=True))
        
        # Query using an indexed field (has_events should be indexed)
        event_chunks = list(chunks_collection.find({"has_events": True, "_test_data": True}))
        self.assertEqual(len(event_chunks), 2)
        
        # Query using compound index (has_events + was_processed_events)
        unprocessed_event_chunks = list(chunks_collection.find(
            {"has_events": True, "was_processed_events": False, "_test_data": True}
        ))
        self.assertEqual(len(unprocessed_event_chunks), 2)


if __name__ == "__main__":
    unittest.main() 