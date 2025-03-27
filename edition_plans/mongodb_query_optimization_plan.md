# MongoDB Query Optimization Plan for alphasync_db

## Overview
This plan outlines the steps to optimize MongoDB queries for the alphasync_db database, focusing on improving performance for event processing, data retrieval, and vector search operations.

## Phase 1: Investigation and Analysis

### 1. Database Structure Analysis

#### Identified Collections in alphasync_db:
- **emails**: Stores email data including metadata and content
- **chunks**: Stores content chunks extracted from emails/documents
- **events**: Stores events extracted from chunks
- **companies**: Stores company information
- **sources**: Stores information about data sources
- **infos**: Stores information extracted from documents

#### Schema Analysis:

**Email Collection Schema:**
```
id: str (ObjectId)
message_id: str (unique email identifier)
conversation_id: str
from_address: str
subject: str
body: str
received_at: datetime
attachments: List[Attachment]
was_processed: bool
relevant: Optional[bool]
```

**Chunk Collection Schema:**
```
id: str (ObjectId)
content: str
summary: str
subject: Optional[str]
source: str
instrument_ids: Optional[List[str]]
embedding: List[float]
include: bool
has_events: bool
document_id: str
document_collection: str
index: int
published_at: datetime
created_at: datetime
was_processed: bool
was_processed_events: bool
```

**Event Collection Schema:**
```
id: str (ObjectId)
name: str
description: str
date: Optional[datetime]
original_date_text: str
location: Optional[str]
event_type: str
companies_ids: List[str]
chunk_ids: List[str]
source: str
confirmed: bool
confidence: float
embedding: List[float]
created_at: datetime
last_updated: datetime
```

**Info Collection Schema:**
```
id: str (ObjectId)
embedding: List[float]
chunk_ids: List[str]
created_at: datetime
last_updated: datetime
companiesId: List[str]
sourcesId: List[str]
```

**Companies Collection Schema:**
```
id: str (ObjectId)
name: str
ticker: str
public: bool
parent_company: str
description: str
sector: Optional[str]
```

#### Collection Relationships:
- Emails → Chunks (one-to-many): An email is broken down into multiple chunks
- Chunks → Events (many-to-many): A chunk can contain multiple events, and an event can span multiple chunks
- Chunks → Companies (many-to-many): Chunks can be associated with multiple companies
- Events → Companies (many-to-many): Events can involve multiple companies
- Info → Chunks (one-to-many): An info is associated with multiple chunks
- Info → Companies (many-to-many): Info can be relevant to multiple companies
- Info → Sources (many-to-many): Info can come from multiple sources

### 2. Query Pattern Analysis

#### High-Frequency Queries:

1. **Chunks Collection Queries:**
   ```python
   # High-frequency query in event processing
   chunks_collection.find({"has_events": True})
   
   # Marking chunks as processed
   chunks_collection.update_one({"_id": chunk_id}, {"$set": {"was_processed_events": True}})
   ```

2. **Events Collection Queries:**
   ```python
   # Vector search for similar events
   events_collection.aggregate([
       {
           "$vectorSearch": {
               "index": "vector_index_loop_events",
               "path": "embedding",
               "queryVector": embedding,
               "numCandidates": 20,
               "limit": 20,
           }
       },
       {
           "$project": {
               "similarityScore": {"$meta": "vectorSearchScore"},
               "document": "$$ROOT",
           }
       },
   ])
   
   # Finding events by ID
   events_collection.find_one({"_id": event_id})
   
   # Updating events with new information
   events_collection.update_one({"_id": event_id}, update_ops)
   
   # Creating new events
   events_collection.insert_one(new_event.model_dump(by_alias=True))
   ```

3. **Companies Collection Queries:**
   ```python
   # Looking up companies information
   companies_collection.find_one(...)  # Exact pattern not visible in the examined code
   ```

### 3. Indexing Analysis

#### Existing Indexes:
- **vector_index_loop_events**: Vector index on events collection for `embedding` field
- No other explicit index creation found in the code except for a compound index `[("infoId", 1), ("userId", 1)]` on a posts collection (not related to alphasync_db)

#### Missing Indexes:
- **Chunks Collection:**
  - No index on `has_events` and `was_processed_events` fields despite frequent queries
  - No index on `document_id` and `document_collection` fields
  - No index on time-based fields (`published_at`, `created_at`)

- **Events Collection:**
  - No conventional indexes on `companies_ids` for company-based lookups
  - No index on `date` for time-based filtering
  - No index on `event_type` for type-based filtering

- **Email Collection:**
  - No index on `message_id` for deduplication checks
  - No index on `received_at` for time-based queries

### 4. Workload Analysis

Based on code examination, the workload appears to be:

- **Read-heavy for chunks collection**: Many queries to find chunks with `has_events: true`
- **Read-heavy for events collection**: Vector searches to find similar events
- **Write-intensive for events collection**: Creating new events and updating existing ones
- **Update operations on chunks**: Marking chunks as processed after event extraction

The system follows a pipeline pattern where:
1. Emails are ingested
2. Chunks are extracted and processed
3. Events are extracted from chunks
4. Events are deduplicated and enriched

### 5. Initial Recommendations

Based on the analysis, here are the initial recommendations:

1. **Indexing Strategy:**
   - Create indexes for all identified missing indexes
   - Prioritize indexes on frequently queried fields (`has_events`, `was_processed_events`)
   - Ensure vector indexes are properly configured

2. **Query Optimization:**
   - Implement batch processing for chunk processing
   - Add projections to reduce data transfer in high-frequency queries
   - Optimize vector searches with appropriate filters

3. **Write Optimization:**
   - Implement bulk operations for event updates and inserts
   - Consider write concern adjustments based on durability requirements

4. **Connection Optimization:**
   - Implement connection pooling
   - Configure optimal read preferences

## Phase 2: Implementation

### 1. Create Critical Indexes for Query Performance ✅ COMPLETED

All necessary indexes have been implemented in order of priority, with comprehensive documentation and error handling.

1. **High-Priority Indexes**:
   - Chunks Collection: Focus on the most frequent query pattern
     ```
     // Primary query for event processing
     { "has_events": 1 }
     
     // Combined index for filtering by both flags
     { "has_events": 1, "was_processed_events": 1 }
     ```

2. **Medium-Priority Indexes**:
   - Events Collection:
     ```
     // Date-based filtering for vector searches and reporting
     { "date": 1 }
     
     // Support for company-based event lookups
     { "companies_ids": 1 }
     ```
   - Emails Collection:
     ```
     // Unique constraint for message deduplication
     { "message_id": 1 }
     ```

3. **Lower-Priority Indexes**:
   - Additional filtering and sorting support:
     ```
     // Chunks Collection
     { "document_id": 1, "document_collection": 1 }
     { "published_at": -1 }
     
     // Events Collection
     { "event_type": 1 }
     { "companies_ids": 1, "date": 1 }
     
     // Emails Collection
     { "received_at": -1 }
     
     // Companies Collection
     { "name": "text" }
     ```

**Implementation Details**:
- Created unified script: `scripts/create_mongodb_indexes.py`
- Implemented all indexes with background option to prevent blocking operations
- Added error handling and connection management for reliability
- Added comprehensive documentation: `docs/mongodb_optimization.md`

### 2. Optimize Core Query Patterns

1. **Implement Batch Processing for `_process_events`**:
   - Update the main event processing loop to process chunks in batches
   - Add batch_size parameter to query cursor
   ```python
   # Current implementation
   for chunk_doc in chunks_collection.find(query):
       # Process each chunk
   
   # Optimized implementation with batching
   batch_size = 100  # Adjust based on average chunk size
   cursor = chunks_collection.find(query).batch_size(batch_size)
   for chunk_doc in cursor:
       # Process each chunk
   ```

2. **Add Projections to Reduce Data Transfer**:
   - Modify high-volume queries to only return needed fields
   ```python
   # Current implementation
   chunks_collection.find({"has_events": True})
   
   # Optimized implementation with projection
   chunks_collection.find(
       {"has_events": True},
       projection={
           "content": 1, 
           "subject": 1, 
           "source": 1, 
           "instrument_ids": 1,
           "embedding": 1,
           "published_at": 1,
           "document_id": 1,
           "document_collection": 1
       }
   )
   ```

3. **Optimize Vector Search in `find_similar_events`**:
   - Add date filtering to reduce search space
   - Add projection to return only required fields
   ```python
   # Current implementation (simplified)
   events_collection.aggregate([
       {
           "$vectorSearch": {
               "index": "vector_index_loop_events",
               "path": "embedding",
               "queryVector": embedding,
               "numCandidates": 20,
               "limit": 20,
           }
       },
       {
           "$project": {
               "similarityScore": {"$meta": "vectorSearchScore"},
               "document": "$$ROOT",
           }
       },
   ])
   
   # Optimized implementation with filtering
   events_collection.aggregate([
       {
           "$vectorSearch": {
               "index": "vector_index_loop_events",
               "path": "embedding",
               "queryVector": embedding,
               "numCandidates": 100,  # Increased to ensure good candidates
               "limit": 20,
               "filter": {  # Add appropriate date filter
                   "created_at": {"$gte": datetime.now() - timedelta(days=30)}
               }
           }
       },
       {
           "$project": {  # Only return necessary fields
               "similarityScore": {"$meta": "vectorSearchScore"},
               "_id": 1,
               "name": 1,
               "description": 1,
               "date": 1,
               "original_date_text": 1,
               "location": 1,
               "event_type": 1,
               "companies_ids": 1,
               "confidence": 1
           }
       },
   ])
   ```

### 3. Implement Bulk Operations for Updates

1. **Create a Bulk Operation Helper Function**:
   ```python
   def perform_bulk_updates(collection, operations):
       """
       Execute bulk write operations on a MongoDB collection.
       
       Args:
           collection: MongoDB collection
           operations: List of pymongo update operations
       
       Returns:
           BulkWriteResult object
       """
       if not operations:
           return None
       
       try:
           result = collection.bulk_write(operations)
           return result
       except Exception as e:
           logger.error(f"Bulk write operation failed: {str(e)}")
           raise
   ```

2. **Refactor Event Update Logic**:
   - Collect updates in a batch and execute them together
   ```python
   # Track bulk operations for events
   event_bulk_ops = []
   
   # Process chunks
   for chunk_doc in chunks_collection.find(query).batch_size(batch_size):
       # Process events...
       
       # Instead of immediate updates
       for event_data in extracted_events:
           if event_data.get("already_exists"):
               event_id = event_data.get("id")
               # ...
               
               # Add to bulk operations instead of immediate update
               event_bulk_ops.append(
                   pymongo.UpdateOne(
                       {"_id": event_id},
                       update_ops
                   )
               )
               
               # Execute in batches of 100
               if len(event_bulk_ops) >= 100:
                   perform_bulk_updates(events_collection, event_bulk_ops)
                   event_bulk_ops = []
   
   # Process any remaining operations
   if event_bulk_ops:
       perform_bulk_updates(events_collection, event_bulk_ops)
   ```

3. **Implement Bulk Chunk Updates**:
   - Group chunk status updates into batches
   ```python
   # Track bulk operations for chunks
   chunk_bulk_ops = []
   
   # After processing each chunk
   chunk_bulk_ops.append(
       pymongo.UpdateOne(
           {"_id": chunk.id},
           {"$set": {"was_processed_events": True}}
       )
   )
   
   # Execute in batches of 100
   if len(chunk_bulk_ops) >= 100:
       perform_bulk_updates(chunks_collection, chunk_bulk_ops)
       chunk_bulk_ops = []
   ```

### 4. Optimize Connection and Client Configuration

1. **Implement Optimized MongoDB Client Function**:
   ```python
   def get_optimized_mongo_client(max_pool_size=50, min_pool_size=10, max_idle_time_ms=30000):
       """
       Create an optimized MongoDB client with proper connection pooling.
       
       Args:
           max_pool_size: Maximum connections in the pool
           min_pool_size: Minimum connections in the pool
           max_idle_time_ms: Maximum time a connection can remain idle
           
       Returns:
           Configured MongoDB client
       """
       mongo_uri = env.MONGO_DB_URL
       client = MongoClient(
           mongo_uri,
           maxPoolSize=max_pool_size,
           minPoolSize=min_pool_size,
           maxIdleTimeMS=max_idle_time_ms
       )
       return client
   ```

2. **Update Collection Access Function**:
   ```python
   def get_optimized_mongo_collection(db_name="alphasync_db", collection_name="emails", 
                                    read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED):
       """
       Get a MongoDB collection with optimized configuration.
       
       Args:
           db_name: Database name
           collection_name: Collection name
           read_preference: ReadPreference for query routing
           
       Returns:
           Configured MongoDB collection
       """
       client = get_optimized_mongo_client()
       db = client[db_name]
       return db.get_collection(
           collection_name, 
           read_preference=read_preference
       )
   ```

## Phase 3: Testing and Edge Cases

### 1. Performance Baseline Measurement

1. **Establish Performance Baselines**:
   - Create a script to measure current query performance:
   ```python
   def measure_query_performance(query_func, iterations=10):
       """Measure performance of a query function"""
       total_time = 0
       results = []
       
       for i in range(iterations):
           start_time = time.time()
           result = query_func()
           end_time = time.time()
           
           execution_time = end_time - start_time
           total_time += execution_time
           results.append(execution_time)
       
       return {
           "avg_time": total_time / iterations,
           "min_time": min(results),
           "max_time": max(results),
           "iterations": iterations
       }
   ```

2. **Measure Key Query Performance**:
   - Test the most critical queries:
   ```python
   # Test chunk query performance
   def test_chunks_query():
       chunks = list(chunks_collection.find({"has_events": True}))
       return len(chunks)
       
   # Test vector search performance
   def test_vector_search():
       embedding = get_embedding("test event search")
       results = find_similar_events("test event search", events_collection)
       return len(results)
   
   # Record performance metrics
   chunk_query_perf = measure_query_performance(test_chunks_query)
   vector_search_perf = measure_query_performance(test_vector_search)
   ```

3. **Measure Event Processing Pipeline**:
   - Time the entire event processing function:
   ```python
   start_time = time.time()
   stats = _process_events()
   end_time = time.time()
   
   total_time = end_time - start_time
   avg_time_per_chunk = total_time / stats["chunks_processed"]
   ```

### 2. Index Effectiveness Validation

1. **Verify Index Usage**:
   - Create a function to check if queries are using indexes:
   ```python
   def validate_index_usage(query, collection):
       """Validate that a query is using indexes properly"""
       explanation = collection.find(query).explain()
       
       # Check for COLLSCAN (collection scan) vs IXSCAN (index scan)
       if explanation["queryPlanner"]["winningPlan"].get("stage") == "COLLSCAN":
           logger.warning(f"Query not using an index: {query}")
           return False
       return True
   ```

2. **Test Critical Queries**:
   ```python
   # Test index usage for the main event processing query
   validate_index_usage({"has_events": True}, chunks_collection)
   
   # Test index usage for company-based lookups
   validate_index_usage({"companies_ids": "COMPANY_ID_HERE"}, events_collection)
   ```

### 3. Edge Case Testing

1. **Test with Large Result Sets**:
   - Create a test function that handles an artificially large result set:
   ```python
   def test_large_result_handling():
       # Create a query that would return a large number of documents
       large_query = {"has_events": True}
       
       # Test with different batch sizes
       for batch_size in [10, 50, 100, 500]:
           start_time = time.time()
           cursor = chunks_collection.find(large_query).batch_size(batch_size)
           count = 0
           
           # Process in batches to simulate real use
           for doc in cursor:
               count += 1
               
           end_time = time.time()
           print(f"Batch size {batch_size}: {count} docs in {end_time - start_time:.2f}s")
   ```

2. **Test with High Concurrency**:
   - Create a concurrent testing function:
   ```python
   def test_concurrent_queries(num_threads=10):
       """Test database performance under concurrent load"""
       import threading
       
       results = []
       threads = []
       
       def worker(worker_id):
           start_time = time.time()
           chunks = list(chunks_collection.find({"has_events": True}).limit(50))
           end_time = time.time()
           results.append({
               "worker_id": worker_id,
               "time": end_time - start_time,
               "docs_found": len(chunks)
           })
       
       # Create and start threads
       for i in range(num_threads):
           t = threading.Thread(target=worker, args=(i,))
           threads.append(t)
           t.start()
           
       # Wait for all threads to complete
       for t in threads:
           t.join()
           
       # Calculate statistics
       total_time = sum(r["time"] for r in results)
       avg_time = total_time / num_threads
       return avg_time
   ```

### 4. Regression Testing

1. **Create Full Pipeline Test**:
   - Develop a test script that executes the entire event extraction pipeline:
   ```python
   def test_event_pipeline_regression():
       """Test the entire event pipeline for regressions"""
       # Create test data
       test_chunk = Chunk(
           content="Test company XYZ will have an earnings call on March 15, 2024",
           summary="Earnings call announcement",
           subject="Upcoming Events",
           source="test_script",
           instrument_ids=["XYZ123"],
           embedding=get_embedding("Test event data"),
           include=True,
           has_events=True,
           document_id="test_doc_123",
           document_collection="test",
           index=1
       )
       
       # Insert test chunk
       chunks_collection.insert_one(test_chunk.model_dump(by_alias=True))
       
       # Run event processing
       stats = _process_events()
       
       # Verify event was created
       events = list(events_collection.find({"source": "test_script"}))
       
       # Clean up test data
       chunks_collection.delete_one({"_id": test_chunk.id})
       for event in events:
           events_collection.delete_one({"_id": event["_id"]})
           
       return {
           "success": len(events) > 0,
           "events_found": len(events),
           "stats": stats
       }
   ```

2. **Test Event Deduplication Logic**:
   - Create a test for the event deduplication functionality:
   ```python
   def test_event_deduplication():
       """Test that event deduplication works properly"""
       # Create a unique test event name
       test_event_name = f"Test Event {uuid.uuid4()}"
       
       # Create and insert test event
       event1 = Event(
           name=test_event_name,
           description="Initial description",
           original_date_text="March 1, 2024",
           event_type="earnings_call",
           companies_ids=["XYZ123"],
           chunk_ids=["chunk1"],
           source="test_script",
           embedding=get_embedding(f"{test_event_name} Initial description")
       )
       events_collection.insert_one(event1.model_dump(by_alias=True))
       
       # Search for similar events
       similar_events = find_similar_events(
           f"{test_event_name} Updated description",
           events_collection
       )
       
       # Clean up test data
       events_collection.delete_one({"_id": event1.id})
       
       return {
           "deduplication_working": len(similar_events) > 0,
           "similarity_score": similar_events[0].get("_similarity_score") if similar_events else 0
       }
   ```

### 5. Production Monitoring Setup

1. **Create Index Size Monitoring**:
   ```python
   def monitor_index_sizes():
       """Monitor index sizes to track growth over time"""
       client = get_optimized_mongo_client()
       db = client["alphasync_db"]
       
       # Get collection stats for each collection
       collections = ["emails", "chunks", "events", "companies", "sources", "infos"]
       stats = {}
       
       for collection_name in collections:
           stats[collection_name] = db.command("collStats", collection_name)
       
       return {
           coll: {
               "count": stats[coll]["count"],
               "size_mb": stats[coll]["size"] / (1024 * 1024),
               "index_size_mb": stats[coll]["totalIndexSize"] / (1024 * 1024),
               "indexes": stats[coll]["indexSizes"]
           } for coll in collections
       }
   ```

2. **Create Query Performance Monitor**:
   ```python
   def setup_query_profiling(level=1):
       """
       Set up MongoDB profiling to track slow queries
       
       Args:
           level: 0=off, 1=slow queries, 2=all queries
       """
       client = get_optimized_mongo_client()
       db = client["alphasync_db"]
       
       # Set profiling level (1 = log slow queries)
       db.set_profiling_level(level)
       
       # Configure slow query threshold (in ms)
       db.command({"profile": level, "slowms": 100})
   ```

## Phase 4: Documentation and Cleanup

1. **Document All Changes**
   - Create comprehensive documentation of all indexes
   - Document query optimization strategies
   - Update codebase comments for optimized queries

2. **Create Monitoring Plan**
   - Set up MongoDB query profiling
   - Implement index usage monitoring
   - Create dashboard for query performance metrics

3. **Knowledge Transfer**
   - Document best practices for future query development
   - Create guidelines for when to use indexes
   - Document bulk operation patterns

4. **Code Cleanup**
   - Remove any temporary code
   - Consolidate helper functions
   - Ensure consistent error handling

5. **Future Optimization Roadmap**
   - Identify areas for future improvement
   - Document sharding strategy if needed
   - Plan for regular index maintenance 