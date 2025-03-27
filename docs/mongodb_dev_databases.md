# MongoDB Development Databases

This document provides information about the MongoDB development database setup for `alphasync_db` and `STKFeed`. These development databases (`alphasync_db_dev` and `STKFeed_dev`) mirror the structure of their production counterparts but don't contain any actual data.

## Purpose

The development databases serve several purposes:

1. **Safe Environment for Testing**: A separate database environment where developers can test changes without affecting production data.
2. **Local Development**: Allows developers to work with a database structure identical to production but without copying sensitive data.
3. **CI/CD Pipeline Integration**: Provides a consistent testing environment for continuous integration and deployment pipelines.
4. **Index Performance Testing**: Enables testing index effectiveness without affecting production database performance.

## Usage

### Switching Between Production and Development

The system uses the `USE_DEV_MONGO_DB` environment variable to determine which database to use:

```python
# In .env file
USE_DEV_MONGO_DB=True  # Use development databases
# or
USE_DEV_MONGO_DB=False  # Use production databases (default)
```

When this variable is set to `True`, the application automatically redirects all database operations to the development databases. This works by appending `_dev` to database names.

### Code Example

All database connections in the application code have been updated to use the utility functions in `util/mongodb_utils.py`:

```python
from util.mongodb_utils import get_mongo_collection

# This will use either "alphasync_db" or "alphasync_db_dev" based on environment setting
collection = get_mongo_collection("emails", "alphasync_db")

# For async operations
from util.mongodb_utils import get_async_database
db = get_async_database("alphasync_db")
```

## Setting Up Development Databases

### Initial Setup

Use the cloning script to create the development databases with identical structure:

```bash
python scripts/clone_db_structure.py
```

This script will:
1. Create all collections in both development databases
2. Clone all standard indexes from production to development
3. Provide instructions for manually creating vector search indexes

### Vector Search Indexes

MongoDB Atlas vector search indexes must be created manually through the Atlas UI:

1. Log in to [MongoDB Atlas](https://cloud.mongodb.com)
2. Navigate to your cluster > Database > Atlas Search
3. Create the following vector indexes on the development databases:

**For alphasync_db_dev:**
1. **vector_index_loop_events**
   - Collection: events
   - Field mapped: embedding
   - Dimensions: 1536
   - Similarity: cosine

2. **vector_index_loop_infos**
   - Collection: infos
   - Field mapped: embedding
   - Dimensions: 1536
   - Similarity: cosine

3. **vector_index**
   - Collection: companies
   - Field mapped: embedding
   - Dimensions: 1536
   - Similarity: cosine

4. **vector_index_loop_sources**
   - Collection: sources
   - Field mapped: embedding
   - Dimensions: 1536
   - Similarity: cosine

### Verifying Setup

After setting up the development databases, you can verify they're working correctly:

```bash
# Check if databases exist
USE_DEV_MONGO_DB=True python -c "from util.mongodb_utils import get_mongo_client; client = get_mongo_client(); print(client.list_database_names())"

# Check if indexes exist
python scripts/check_indexes.py

# Check vector search functionality
python scripts/check_vector_indexes.py
```

## Best Practices

### Database Operations

1. **Always Use Utility Functions**: Use the functions in `util/mongodb_utils.py` for all database operations.
2. **Test Both Environments**: Test your code with both `USE_DEV_MONGO_DB=True` and `USE_DEV_MONGO_DB=False`.
3. **Clean Up Test Data**: Always clean up test data after testing in the development database.

### Development Workflow

1. Set `USE_DEV_MONGO_DB=True` in your local `.env` file
2. Run the application or tests against the development database
3. Verify functionality works as expected
4. Before deployment, test once more with `USE_DEV_MONGO_DB=False` to ensure compatibility with production

## Limitations and Considerations

### Performance Differences

Development databases may perform differently from production due to:

1. **Data Volume**: Development databases typically have less data, which can make queries faster
2. **Resource Allocation**: Production databases often have more compute resources
3. **Index Warm-up**: Production indexes might be "warm" due to repeated usage patterns

### MongoDB Atlas Free Tier Considerations

If using MongoDB Atlas Free Tier:

1. **Storage Limitation**: Free tier limits storage to 512MB, which means you should be careful about data volume
2. **Connection Limitation**: Limited concurrent connections
3. **Compute Limitation**: Shared resources may affect performance

### Schema Synchronization

Development and production database schemas need to be kept in sync:

1. **Run Clone Script Periodically**: Re-run the cloning script when the production schema changes
2. **Check for Index Drift**: Verify periodically that development and production indexes match
3. **Vector Index Manual Updates**: Remember to manually update vector indexes when needed

## Troubleshooting

### Common Issues

1. **"No such database" errors**: Ensure the development databases have been created with the cloning script
2. **Vector search failures**: Vector indexes need to be created manually in MongoDB Atlas UI
3. **Performance issues**: Consider adding more indexes or optimizing queries for large datasets

### Debugging Tips

1. **Check Current Database**: Print the database name to verify which environment you're using
2. **Examine Indexes**: Use `db.collection.getIndexes()` in MongoDB shell to verify indexes
3. **Query Explanation**: Use `explain()` to check if queries are using the expected indexes

## Maintenance

### Periodic Tasks

1. **Re-sync Development Structure**: Run the cloning script periodically to catch schema changes
2. **Update Documentation**: Keep this documentation current with any changes to database structure
3. **Monitor Storage Usage**: Check storage usage periodically, especially on MongoDB Atlas Free Tier 