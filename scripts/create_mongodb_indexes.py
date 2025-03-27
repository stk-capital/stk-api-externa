#!/usr/bin/env python
"""
MongoDB Index Creation Script for alphasync_db

This script creates all necessary indexes for the alphasync_db database
to optimize query performance without disrupting existing operations.
"""

import logging
import time
import os
import sys
from pymongo import MongoClient, IndexModel
from pymongo.errors import OperationFailure
from dotenv import load_dotenv

# Add the project root to the path to import the utility module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util.mongodb_utils import get_mongo_client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# This function is kept for backward compatibility
def get_mongo_client():
    """Establish connection to MongoDB with appropriate options.
    
    Note: This is a wrapper around the utility module's get_mongo_client function
    for backward compatibility.
    """
    # Import the utility module for database connections
    from util.mongodb_utils import get_mongo_client as get_client
    return get_client(
        timeout_ms=30000,
        connect_timeout_ms=30000,
        socket_timeout_ms=60000
    )

def create_index_safely(collection, index_spec, index_name=None, background=True, unique=False):
    """
    Create an index on a collection in a safe manner.
    
    Args:
        collection: MongoDB collection
        index_spec: Index specification
        index_name: Optional name for the index
        background: Whether to create the index in the background
        unique: Whether the index should enforce uniqueness
    
    Returns:
        str: Name of the created index or None if creation failed
    """
    start_time = time.time()
    options = {"background": background}
    
    if index_name:
        options["name"] = index_name
        
    if unique:
        options["unique"] = True
    
    try:
        result = collection.create_index(index_spec, **options)
        end_time = time.time()
        logger.info(
            f"Created index '{result}' on {collection.name} "
            f"with spec {index_spec} in {end_time - start_time:.2f}s"
        )
        return result
    except OperationFailure as e:
        logger.error(f"Failed to create index on {collection.name}: {str(e)}")
        return None

def create_all_indexes():
    """Create all indexes for alphasync_db collections based on priority levels."""
    client = get_mongo_client()
    db = client["alphasync_db"]
    
    # Get collection references
    chunks_collection = db["chunks"]
    events_collection = db["events"]
    emails_collection = db["emails"]
    companies_collection = db["companies"]
    
    try:
        # ------------------------------------------------------------------
        # 1. High-Priority Indexes
        # ------------------------------------------------------------------
        logger.info("Creating HIGH-PRIORITY indexes...")
        
        # Chunks Collection - Primary query pattern
        logger.info("Creating index on chunks collection for has_events field...")
        create_index_safely(
            chunks_collection, 
            [("has_events", 1)],
            "idx_chunks_has_events"
        )
        
        # Chunks Collection - Combined index for both flags
        logger.info("Creating compound index on chunks collection for has_events and was_processed_events fields...")
        create_index_safely(
            chunks_collection, 
            [("has_events", 1), ("was_processed_events", 1)],
            "idx_chunks_has_events_was_processed"
        )
        
        # ------------------------------------------------------------------
        # 2. Medium-Priority Indexes
        # ------------------------------------------------------------------
        logger.info("Creating MEDIUM-PRIORITY indexes...")
        
        # Events Collection - Date-based filtering for vector searches and reporting
        logger.info("Creating index on events collection for date field...")
        create_index_safely(
            events_collection,
            [("date", 1)],
            "idx_events_date"
        )
        
        # Events Collection - Support for company-based event lookups
        logger.info("Creating index on events collection for companies_ids field...")
        create_index_safely(
            events_collection,
            [("companies_ids", 1)],
            "idx_events_companies_ids"
        )
        
        # Emails Collection - Support for deduplication checks
        logger.info("Creating unique index on emails collection for message_id field...")
        create_index_safely(
            emails_collection,
            [("message_id", 1)],
            "idx_emails_message_id",
            unique=True
        )
        
        # ------------------------------------------------------------------
        # 3. Lower-Priority Indexes
        # ------------------------------------------------------------------
        logger.info("Creating LOWER-PRIORITY indexes...")
        
        # Chunks Collection - Document lookup
        logger.info("Creating compound index on chunks collection for document_id and document_collection fields...")
        create_index_safely(
            chunks_collection,
            [("document_id", 1), ("document_collection", 1)],
            "idx_chunks_document"
        )
        
        # Chunks Collection - Time-based queries
        logger.info("Creating index on chunks collection for published_at field...")
        create_index_safely(
            chunks_collection,
            [("published_at", -1)],
            "idx_chunks_published_at"
        )
        
        # Events Collection - Event type filtering
        logger.info("Creating index on events collection for event_type field...")
        create_index_safely(
            events_collection,
            [("event_type", 1)],
            "idx_events_event_type"
        )
        
        # Events Collection - Combined company and date filtering
        logger.info("Creating compound index on events collection for companies_ids and date fields...")
        create_index_safely(
            events_collection,
            [("companies_ids", 1), ("date", 1)],
            "idx_events_companies_date"
        )
        
        # Emails Collection - Time-based queries
        logger.info("Creating index on emails collection for received_at field...")
        create_index_safely(
            emails_collection,
            [("received_at", -1)],
            "idx_emails_received_at"
        )
        
        # Companies Collection - Text search on company names
        logger.info("Creating text index on companies collection for name field...")
        create_index_safely(
            companies_collection,
            [("name", "text")],
            "idx_companies_name_text"
        )
        
        logger.info("All index creation completed successfully")
        
    except Exception as e:
        logger.error(f"Error creating indexes: {e}")
    finally:
        # Close the connection
        client.close()

if __name__ == "__main__":
    logger.info("Starting index creation for alphasync_db")
    create_all_indexes()
    logger.info("Index creation operation completed") 