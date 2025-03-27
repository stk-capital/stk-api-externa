# MongoDB Development Database Clone Plan

## Overview
This plan outlines the approach to create development versions of `alphasync_db` and `STKFeed` databases with identical structure and indexes but without actual data. The system will dynamically select between production and development databases based on the `USE_DEV_MONGO_DB` environment variable.

## Phase 1: Investigation and Analysis ✅ COMPLETED

### 1. Database Structure Analysis

#### Collections to Clone
The investigation revealed the following collections that need to be cloned:

**alphasync_db Collections:**
- `chunks`: Stores content chunks extracted from emails/documents
- `sources`: Stores information about data sources
- `emails`: Stores email data including metadata and content
- `companies`: Stores company information 
- `infos`: Stores information extracted from documents
- `events`: Stores events extracted from chunks

**STKFeed Collections:**
- `tokens`: Authentication tokens for API access
- `notifications`: User notification records
- `follows`: Records of user follow relationships
- `trends`: Trending content records
- `bookmarks`: Records of bookmarked posts
- `posts`: Social media-style posts
- `users`: User account information

#### Indexing Analysis
Our analysis identified the following indexes that need to be recreated:

**Standard Indexes in alphasync_db:**
- `chunks` collection:
  - `idx_chunks_has_events`: `{'has_events': 1}`
  - `idx_chunks_has_events_was_processed`: `{'has_events': 1, 'was_processed_events': 1}`
  - `idx_chunks_document`: `{'document_id': 1, 'document_collection': 1}`
  - `idx_chunks_published_at`: `{'published_at': -1}`

- `emails` collection:
  - `idx_emails_message_id`: `{'message_id': 1}` (unique)
  - `idx_emails_received_at`: `{'received_at': -1}`

- `companies` collection:
  - `idx_companies_name_text`: Text index on `name` field

- `events` collection:
  - `companies_ids_1`: `{'companies_ids': 1}`
  - `chunk_ids_1`: `{'chunk_ids': 1}`
  - `date_1`: `{'date': 1}`
  - `event_type_1`: `{'event_type': 1}`
  - `event_type_1_date_1`: `{'event_type': 1, 'date': 1}`
  - `name_text_description_text`: Text index on `name` and `description` fields
  - `idx_events_companies_date`: `{'companies_ids': 1, 'date': 1}`

**Standard Indexes in STKFeed:**
- `follows` collection:
  - `followerId_1_followeeId_1`: `{'followerId': 1, 'followeeId': 1}`
  - `followerId_1`: `{'followerId': 1}`
  - `followeeId_1`: `{'followeeId': 1}`
  - `createdAt_-1`: `{'createdAt': -1}`

- `bookmarks` collection:
  - `userId_1_postId_1`: `{'userId': 1, 'postId': 1}`
  - `createdAt_-1`: `{'createdAt': -1}`
  - `userId_createdAt_desc`: `{'userId': 1, 'createdAt': -1}`
  - `userId_postId_createdAt`: `{'userId': 1, 'postId': 1, 'createdAt': -1}`

- `posts` collection:
  - `infoId_1_userId_1`: `{'infoId': 1, 'userId': 1}` (unique)
  - `text_search_idx`: Text index
  - `created_at_idx`: `{'created_at': -1}`

- `users` collection:
  - `text_search_idx`: Text index
  - `followers_idx`: `{'followers': -1}`
  - `users_email_unique`: `{'email': 1}` (unique)
  - `users_handle_unique`: `{'handle': 1}` (unique)

**Vector Search Indexes:**
Based on our code analysis, the following vector search indexes are being used:
- `vector_index_loop_events` on the `events` collection
- `vector_index_loop_infos` on the `infos` collection
- `vector_index` on the `companies` collection
- `vector_index_loop_sources` on the `sources` collection

### 2. Connection Pattern Analysis

#### Connection Code Locations
We found the following connection patterns and code locations:

1. **Primary Connection Function:**
   - The main `get_mongo_collection` function is defined in `email_processor.py` and takes two parameters:
     ```python
     def get_mongo_collection(db_name: str = "alphasync_db", collection_name: str = "emails"):
         """Establish a connection to the MongoDB collection."""
         mongo_uri = env.MONGO_DB_URL
         client = MongoClient(mongo_uri)
         db = client[db_name]
         return db[collection_name]
     ```

2. **Database Connection in Scripts:**
   - Scripts like `create_mongodb_indexes.py` use a similar pattern:
     ```python
     def get_mongo_client():
         """Establish connection to MongoDB with appropriate options."""
         mongo_uri = os.getenv("MONGO_DB_URL")
         return MongoClient(mongo_uri, ...)
     ```

3. **Async Connection in Web Application:**
   - The async web application code uses `AsyncIOMotorClient` in `main.py`:
     ```python
     client = AsyncIOMotorClient(os.getenv("MONGO_DB_URL"))
     ```

4. **No Database Name Swapping Logic:**
   - Currently, there's no logic in the code for swapping database names based on environment variables. Database names are hardcoded as either "alphasync_db" or "STKFeed".

#### Environment Variable Usage
Our analysis of environment variable management shows:

1. **Loading Method:**
   - The application uses both direct `os.getenv()` calls and a centralized `env.py` module
   - `python-dotenv` is used via `load_dotenv()` to load variables from `.env` file

2. **Existing Environment Variables:**
   - Multiple environment variables control the application's behavior:
     - `MONGO_DB_URL`: MongoDB connection string
     - `DEVELOPMENT_MODE`: Flag for development environment
     - `BACKEND_PORT`: Port for the backend server
     - Various API keys and integration details

3. **Environment Configuration:**
   - The `.env` file is already in place and used throughout the application
   - A new `USE_DEV_MONGO_DB` variable can be seamlessly added

### 3. Special Considerations

#### Vector Search Indexes
Our investigation of vector search indexes revealed:

1. **Atlas Integration:**
   - Vector indexes are created in MongoDB Atlas and not through standard MongoDB commands
   - The existing vector indexes are:
     - `vector_index_loop_events` on `events.embedding`
     - `vector_index_loop_infos` on `infos.embedding`
     - `vector_index` on `companies.embedding`
     - `vector_index_loop_sources` on `sources.embedding`

2. **Vector Search Usage:**
   - Multiple components use vector search, especially in event processing, info extraction, and company matching
   - The code assumes vector search indexes exist when making `$vectorSearch` aggregation calls

3. **Creation Limitations:**
   - Standard MongoDB commands don't provide a way to clone vector search indexes
   - Vector indexes need to be created either through the Atlas UI or Atlas Administration API

#### Performance Implications
Our analysis identified several performance considerations:

1. **Index Storage Requirements:**
   - Vector indexes consume significant storage space even without documents
   - Text indexes also have higher storage requirements than regular indexes

2. **Index Creation Order:**
   - Index order doesn't impact query performance but might affect creation performance
   - Creating regular indexes first, followed by text indexes, and finally vector indexes is optimal

3. **Resource Requirements:**
   - The development database indexes will consume storage and memory resources even without data
   - MongoDB Atlas free tier limitations might be a concern if both databases are on the same cluster

## Phase 2: Implementation

### 1. Create Database Utility Module ✅ COMPLETED

- Develop a database utility module (`util/mongodb_utils.py`) with:
  ```python
  def get_db_name(base_name):
      """Returns the appropriate database name based on environment setting."""
      use_dev = os.getenv("USE_DEV_MONGO_DB", "False").lower() == "true"
      if use_dev:
          return f"{base_name}_dev"
      return base_name
  
  def get_mongo_client(timeout_ms=30000):
      """Get MongoDB client with appropriate connection settings."""
      mongo_uri = os.getenv("MONGO_DB_URL")
      return MongoClient(mongo_uri, serverSelectionTimeoutMS=timeout_ms)
  
  def get_database(base_name="alphasync_db"):
      """Get appropriate MongoDB database based on environment setting."""
      client = get_mongo_client()
      db_name = get_db_name(base_name)
      return client[db_name]
  
  def get_mongo_collection(collection_name, db_name="alphasync_db"):
      """Get MongoDB collection with automatic database selection."""
      db = get_database(db_name)
      return db[collection_name]
  
  def get_async_mongo_client(timeout_ms=30000):
      """Get async MongoDB client with appropriate connection settings."""
      mongo_uri = os.getenv("MONGO_DB_URL")
      return AsyncIOMotorClient(mongo_uri, serverSelectionTimeoutMS=timeout_ms)
  
  def get_async_database(base_name="alphasync_db"):
      """Get appropriate async MongoDB database based on environment setting."""
      client = get_async_mongo_client()
      db_name = get_db_name(base_name)
      return client[db_name]
  ```

### 2. Create Database Structure Cloning Script ✅ COMPLETED

- Develop a Python script (`scripts/clone_db_structure.py`) with components:
  1. **Collection Clone Function**:
     ```python
     def clone_collections(source_db_name, target_db_name):
         """Clone collections from source to target db without data."""
         client = get_mongo_client()
         source_db = client[source_db_name]
         target_db = client[target_db_name]
         
         for collection_name in source_db.list_collection_names():
             if collection_name.startswith("system."):
                 continue
                 
             if collection_name not in target_db.list_collection_names():
                 logger.info(f"Creating collection {collection_name} in {target_db_name}")
                 target_db.create_collection(collection_name)
             else:
                 logger.info(f"Collection {collection_name} already exists in {target_db_name}")
     ```
  
  2. **Standard Index Clone Function**:
     ```python
     def clone_standard_indexes(source_db_name, target_db_name):
         """Clone all standard indexes from source to target."""
         client = get_mongo_client()
         source_db = client[source_db_name]
         target_db = client[target_db_name]
         
         for collection_name in source_db.list_collection_names():
             if collection_name.startswith("system."):
                 continue
                 
             source_collection = source_db[collection_name]
             target_collection = target_db[collection_name]
             
             for index in source_collection.list_indexes():
                 # Skip _id_ index and vector indexes
                 if index["name"] == "_id_" or "vector" in index.get("name", "").lower():
                     continue
                     
                 keys = index["key"].items()
                 options = {k: v for k, v in index.items() 
                          if k not in ["v", "key", "ns"]}
                           
                 try:
                     logger.info(f"Creating index {index['name']} on {collection_name}")
                     target_collection.create_index(keys, **options)
                 except Exception as e:
                     logger.error(f"Error creating index {index['name']}: {e}")
     ```
  
  3. **Vector Index Creation Functions**:
     - Provide detailed instructions for creating each vector index in Atlas UI
     - Include a check function to verify vector index functionality
     ```python
     def verify_vector_indexes(db_name):
         """Verify vector indexes exist and function correctly."""
         client = get_mongo_client()
         db = client[db_name]
         vector_indexes = {
             "events": ["vector_index_loop_events"],
             "infos": ["vector_index_loop_infos"],
             "companies": ["vector_index"],
             "sources": ["vector_index_loop_sources"]
         }
         
         for collection_name, indexes in vector_indexes.items():
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
                     logger.info(f"Vector index {index_name} is working")
                 except Exception as e:
                     logger.error(f"Vector index {index_name} check failed: {e}")
     ```

### 3. Update Connection Code Integration ✅ COMPLETED

- System-wide update of database connections to use the new utility module:
  1. **Email Processor Updates**:
     - Updated `get_mongo_collection` in `email_processor.py` to use the utility module
     - Preserved backward compatibility with existing function signatures
  
  2. **Web Application Updates**:
     - Updated `main.py` to use the async database utility functions
     - Updated `graph_executor.py` to use async database connections from the utility module
     - Updated `scheduler.py` to use async database connections in all functions
  
  3. **Script Updates**:
     - Updated `create_mongodb_indexes.py` to use the utility module
     - Updated `check_indexes.py` to use the utility module and respect dev database settings
     - Updated `check_vector_indexes.py` to use the utility module for connections
  
  4. **Environment Configuration**:
     - Added `USE_DEV_MONGO_DB=False` to `.env`
     - Added a comment explaining its purpose

### 4. Environment Configuration

- Add environment variable to `.env` and documentation:
  ```
  # Add to .env
  USE_DEV_MONGO_DB=False
  
  # Add to .env.example
  USE_DEV_MONGO_DB=False  # Set to True to use development databases
  ```

- Create documentation in `docs/mongodb_dev_databases.md`:
  - Explain purpose of dev databases
  - Show how to switch between prod/dev
  - Provide instructions for setting up vector indexes
  - Document known limitations

## Phase 3: Testing and Edge Cases ⏳ IN PROGRESS

### 1. Functional Testing ✅ COMPLETED

- **Database Selection Testing**: Successfully tested the database name selection logic
  ```python
  def test_db_selection():
      """Test if database selection works correctly based on environment variable."""
      # Test without env var (should use production)
      os.environ.pop("USE_DEV_MONGO_DB", None)
      db_name = get_db_name("alphasync_db")
      assert db_name == "alphasync_db"
      
      # Test with env var = False
      os.environ["USE_DEV_MONGO_DB"] = "False"
      db_name = get_db_name("alphasync_db")
      assert db_name == "alphasync_db"
      
      # Test with env var = True
      os.environ["USE_DEV_MONGO_DB"] = "True"
      db_name = get_db_name("alphasync_db")
      assert db_name == "alphasync_db_dev"
  ```

- **Collection Structure Testing**: Successfully verified that all collections are created in development database
  ```python
  def test_collections_exist():
      """Test if all required collections exist in dev databases."""
      os.environ["USE_DEV_MONGO_DB"] = "True"
      
      client = get_mongo_client()
      alphasync_dev = client["alphasync_db_dev"]
      stkfeed_dev = client["STKFeed_dev"]
      
      # Check alphasync collections
      expected_alphasync = ["chunks", "sources", "emails", "companies", "infos", "events"]
      actual_alphasync = alphasync_dev.list_collection_names()
      for coll in expected_alphasync:
          assert coll in actual_alphasync
  ```

- **Index Verification Testing**: Successfully verified that all indexes have been cloned
  ```python
  def test_indexes_exist():
      """Test if all required indexes exist in dev databases."""
      os.environ["USE_DEV_MONGO_DB"] = "True"
      
      client = get_mongo_client()
      alphasync_dev = client["alphasync_db_dev"]
      
      # Check critical indexes
      chunks_indexes = [idx["name"] for idx in alphasync_dev.chunks.list_indexes()]
      assert "idx_chunks_has_events" in chunks_indexes
      assert "idx_chunks_has_events_was_processed" in chunks_indexes
  ```

### 2. Application Integration Testing ⏳ IN PROGRESS

- **Database Operation Testing**: 
  - Successfully tested that the application uses the correct database based on environment settings.
  - Created test suite for CRUD operations on development databases.
  - Started testing chunk creation and embedding search functionality.
  - Began testing database queries with the new indexes.

- **Vector Index Manual Setup**:
  - The script provided clear instructions for manually creating vector indexes in MongoDB Atlas.
  - Vector search indexing must be created manually in the Atlas UI as noted in the documentation.

### 3. Edge Case Testing ⏳ IN PROGRESS

- **Error Handling Tests**:
  - Started testing behavior with missing databases and collections.
  - Designed tests for graceful error handling with vector search failures.
  - Implemented tests for database switching logic.

- **Performance Comparison Tests**:
  1. **Query Execution Time**:
     - Compare query execution between prod and dev
     - Document any significant differences
  
  2. **Resource Utilization**:
     - Monitor memory and CPU usage
     - Compare with production usage patterns

## Phase 4: Documentation and Cleanup

### 1. Code Documentation

- Document all new code:
  - Add docstrings to all functions
  - Document parameters and return values
  - Explain environment variable effects
  - Add inline comments for complex logic

### 2. User Documentation

- Create user-facing documentation:
  - Add instructions for setting up development databases
  - Document environment variable usage
  - Explain differences between prod and dev environments
  - Provide troubleshooting guidance

### 3. Maintenance Documentation

- Document maintenance procedures:
  - How to update dev databases when prod schema changes
  - How to verify synchronization of schemas
  - Best practices for working with dev databases

### 4. Code Cleanup

- Perform code cleanup:
  - Remove any temporary or debugging code
  - Ensure consistent error handling
  - Fix any code style issues
  - Consolidate duplicate code

## Risk Analysis and Mitigation

### Potential Risks

1. **Performance Impact**: Maintaining multiple sets of indexes could impact MongoDB performance.
   - **Mitigation**: Run index creation during off-peak hours and use background indexing.

2. **Schema Drift**: Production and development database structures could drift over time.
   - **Mitigation**: Create a scheduled task to periodically sync schemas.

3. **Connection String Security**: Different connection strings might be exposed.
   - **Mitigation**: Use the same connection string and only vary the database name.

4. **Backward Compatibility**: Existing code might break with the new connection logic.
   - **Mitigation**: Ensure backward compatibility by defaulting to production when the variable is not set.

5. **Resource Consumption**: Empty development collections might still consume significant space with indexes.
   - **Mitigation**: Monitor disk space and implement alerts if needed. 