"""
MongoDB Database Utility Module

This module provides utility functions for MongoDB database connections with support
for dynamically selecting between production and development databases based on
the USE_DEV_MONGO_DB environment variable.
"""

import os
import logging
from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

# Configure logging
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


def get_db_name(base_name):
    """
    Returns the appropriate database name based on environment setting.
    
    Args:
        base_name (str): The base database name (e.g., "alphasync_db" or "STKFeed")
        
    Returns:
        str: The actual database name to use (with "_dev" suffix if in dev mode)
    """
    use_dev = os.getenv("USE_DEV_MONGO_DB", "False").lower() == "true"
    if use_dev:
        dev_name = f"{base_name}_dev"
        logger.debug(f"Using development database: {dev_name}")
        return dev_name
    return base_name


def get_mongo_client(timeout_ms=30000, connect_timeout_ms=30000, socket_timeout_ms=60000):
    """
    Get MongoDB client with appropriate connection settings.
    
    Args:
        timeout_ms (int): Server selection timeout in milliseconds
        connect_timeout_ms (int): Connection timeout in milliseconds
        socket_timeout_ms (int): Socket timeout in milliseconds
        
    Returns:
        MongoClient: Configured MongoDB client
    """
    mongo_uri = os.getenv("MONGO_DB_URL")
    if not mongo_uri:
        logger.error("MongoDB connection URI not found in environment variables")
        raise ValueError("MongoDB connection URI not found in environment variables")
    
    return MongoClient(
        mongo_uri,
        serverSelectionTimeoutMS=timeout_ms,
        connectTimeoutMS=connect_timeout_ms,
        socketTimeoutMS=socket_timeout_ms
    )


def get_database(base_name="alphasync_db"):
    """
    Get appropriate MongoDB database based on environment setting.
    
    Args:
        base_name (str): The base database name
        
    Returns:
        Database: MongoDB database object
    """
    client = get_mongo_client()
    db_name = get_db_name(base_name)
    return client[db_name]


def get_mongo_collection(collection_name, db_name="alphasync_db"):
    """
    Get MongoDB collection with automatic database selection.
    
    This is a drop-in replacement for the existing get_mongo_collection function
    but with support for development database selection.
    
    Args:
        collection_name (str): Name of the collection to access
        db_name (str): Base name of the database
        
    Returns:
        Collection: MongoDB collection object
    """
    db = get_database(db_name)
    return db[collection_name]


def get_async_mongo_client(timeout_ms=30000):
    """
    Get async MongoDB client with appropriate connection settings.
    
    Args:
        timeout_ms (int): Server selection timeout in milliseconds
        
    Returns:
        AsyncIOMotorClient: Configured async MongoDB client
    """
    mongo_uri = os.getenv("MONGO_DB_URL")
    if not mongo_uri:
        logger.error("MongoDB connection URI not found in environment variables")
        raise ValueError("MongoDB connection URI not found in environment variables")
    
    return AsyncIOMotorClient(
        mongo_uri,
        serverSelectionTimeoutMS=timeout_ms
    )


def get_async_database(base_name="alphasync_db"):
    """
    Get appropriate async MongoDB database based on environment setting.
    
    Args:
        base_name (str): The base database name
        
    Returns:
        MotorDatabase: Async MongoDB database object
    """
    client = get_async_mongo_client()
    db_name = get_db_name(base_name)
    return client[db_name]


def get_async_collection(collection_name, db_name="alphasync_db"):
    """
    Get async MongoDB collection with automatic database selection.
    
    Args:
        collection_name (str): Name of the collection to access
        db_name (str): Base name of the database
        
    Returns:
        MotorCollection: Async MongoDB collection object
    """
    db = get_async_database(db_name)
    return db[collection_name] 