# MongoDB Query Optimization

This document describes the MongoDB query optimization strategy implemented for the alphasync_db database.

## Implemented Indexes

The following indexes have been created to optimize query performance across different collections, organized by priority.

### High-Priority Indexes

These indexes address the most critical performance bottlenecks:

#### Chunks Collection

1. **Simple Index on `has_events`**
   - Index: `{ "has_events": 1 }`
   - Name: `idx_chunks_has_events`
   - Purpose: Optimizes the high-frequency query `chunks_collection.find({"has_events": True})` used in event processing.
   - Impact: Significantly reduces query time for the event processing pipeline.

2. **Compound Index on `has_events` and `was_processed_events`**
   - Index: `{ "has_events": 1, "was_processed_events": 1 }`
   - Name: `idx_chunks_has_events_was_processed`
   - Purpose: Optimizes queries that filter on both flags, allowing for efficient selection of chunks that have events but haven't been processed yet.
   - Impact: Improves performance for selective event processing.

### Medium-Priority Indexes

These indexes support frequent but less critical query patterns:

#### Events Collection

1. **Index on `date`**
   - Index: `{ "date": 1 }`
   - Name: `idx_events_date`
   - Purpose: Supports date-based filtering for vector searches and reporting.
   - Impact: Improves performance for time-based event queries.

2. **Index on `companies_ids`**
   - Index: `{ "companies_ids": 1 }`
   - Name: `idx_events_companies_ids`
   - Purpose: Supports lookups of events by company.
   - Impact: Accelerates company-specific event queries.

#### Emails Collection

1. **Unique Index on `message_id`**
   - Index: `{ "message_id": 1 }`
   - Name: `idx_emails_message_id`
   - Purpose: Enforces unique constraints and accelerates email lookups by message ID.
   - Impact: Prevents duplicate emails and speeds up deduplication checks.

### Lower-Priority Indexes

These indexes support less frequent query patterns and additional functionality:

#### Chunks Collection

1. **Compound Index on `document_id` and `document_collection`**
   - Index: `{ "document_id": 1, "document_collection": 1 }`
   - Name: `idx_chunks_document`
   - Purpose: Optimizes lookups of chunks by their source document.
   - Impact: Improves performance when retrieving all chunks from a specific document.

2. **Index on `published_at`**
   - Index: `{ "published_at": -1 }`
   - Name: `idx_chunks_published_at`
   - Purpose: Supports time-based queries and sorting on chunks.
   - Impact: Improves performance for chronological data access.

#### Events Collection

1. **Index on `event_type`**
   - Index: `{ "event_type": 1 }`
   - Name: `idx_events_event_type`
   - Purpose: Supports filtering events by type (e.g., earnings_call, investor_conference).
   - Impact: Accelerates queries that filter by event category.

2. **Compound Index on `companies_ids` and `date`**
   - Index: `{ "companies_ids": 1, "date": 1 }`
   - Name: `idx_events_companies_date`
   - Purpose: Optimizes queries that filter both by company and date.
   - Impact: Improves performance for company-specific calendar views.

#### Emails Collection

1. **Index on `received_at`**
   - Index: `{ "received_at": -1 }`
   - Name: `idx_emails_received_at`
   - Purpose: Supports time-based queries and sorting on emails.
   - Impact: Improves performance for email timeline views.

#### Companies Collection

1. **Text Index on `name`**
   - Index: `{ "name": "text" }`
   - Name: `idx_companies_name_text`
   - Purpose: Enables text search on company names.
   - Impact: Supports fuzzy matching and keyword search for companies.

## Running the Index Creation Script

To create all indexes, run the index creation script:

```bash
python scripts/create_mongodb_indexes.py
```

This script:
- Creates indexes in the background to minimize impact on running operations
- Creates indexes in order of priority (high, medium, low)
- Logs the progress and time taken for each index creation
- Handles errors gracefully if index creation fails

## Index Creation Safety

The index creation script follows these safety practices:

1. **Background Indexing**: All indexes are created with the `background: true` option to prevent blocking database operations.
2. **Connection Timeout Settings**: Extended timeouts are configured to accommodate potentially long-running index builds.
3. **Error Handling**: The script handles failures gracefully and logs detailed error information.
4. **Exception Management**: The script uses try/except blocks to ensure proper cleanup even if errors occur.

## Monitoring Index Performance

After creating indexes, you can monitor their effectiveness using:

```javascript
// Check if a query is using an index
db.chunks.find({"has_events": true}).explain()

// Get index statistics
db.chunks.stats().indexSizes

// Check index size across all collections
db.runCommand({ serverStatus: 1 }).metrics.commands
```

## Performance Considerations

Some indexes may impact write performance. Monitor the following:

1. **Write Impact**: Indexes can slow down inserts and updates, especially unique indexes
2. **Index Size**: Text indexes and indexes on large fields can consume significant memory
3. **Index Intersection**: MongoDB can use multiple indexes for a single query in some cases

## Next Steps

After implementing these indexes, the next optimization steps include:
1. Adding batch processing to event extraction
2. Implementing projections for query optimization
3. Adding bulk operations for updates 