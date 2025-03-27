
from util.mongodb_utils import get_mongo_collection

from env import db_name_alphasync
from models.chunks import Chunk
import logging
from datetime import datetime
from pymongo import errors
from util.companies_utils import intruments_to_companies_ids
from util.embedding_utils import get_embedding
from util.sources_utils import find_similar_source
from models.sources import Source
from models.infos import Info
from util.infos_utils import find_similar_info_vector_search

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _process_chunks():
    """
    Process new chunks by:
      1. Determining associated company and source IDs (existing or new),
      2. Finding an existing Info document via vector search using the chunk's embedding,
         and updating it with the new chunk and associated company/source IDs,
         or creating a new Info if none exists.
      3. Marking the chunk as processed.
    """
    infos_collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="infos")
    chunks_collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="chunks")
    companies_collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="companies")
    sources_collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="sources")

    # Modified query to process only chunks marked as relevant (include=True)
    query = {"was_processed": False, "include": True}
    cursor = chunks_collection.find(query)
    #grab first doc for debugging, its not a list,s o i cant use cursor[0], but it comes from cursor
    if cursor:
        for doc in cursor:
            #doc = list(cursor)[0]
            try:
                chunk = Chunk(**doc)
            except Exception as e:
                logger.error(f"Error parsing chunk: {e}")
                continue

            # Process companies from chunk.instrument_ids
            companies_ids = intruments_to_companies_ids(chunk.instrument_ids, companies_collection)
            logger.info(f"Companies IDs: {companies_ids}")

            # Process source from chunk.source (assumed to be a single string)
            sources_ids = []
            if chunk.source != "":
                try:
                    source_embedding = get_embedding(chunk.source)
                except Exception as e:
                    logger.error(f"Error generating embedding for source '{chunk.source}': {e}")
                    source_embedding = []
                if source_embedding:
                    existing_source = find_similar_source(source_embedding, sources_collection)
                    if existing_source:
                        sources_ids.append(existing_source.id)
                    else:
                        new_source = Source(
                            name=chunk.source,
                            embedding=source_embedding,
                            created_at=datetime.now(),
                        )
                        try:
                            result = sources_collection.insert_one(new_source.model_dump(by_alias=True))
                            new_source.id = result.inserted_id if result.inserted_id else new_source.id
                            sources_ids.append(new_source.id)
                            logger.info(f"Inserted new source '{chunk.source}' with ID: {new_source.id}")
                        except errors.PyMongoError as e:
                            logger.error(f"MongoDB error inserting source '{chunk.source}': {e}")

            # Find an existing Info document using vector search with chunk.embedding
            similar_info = find_similar_info_vector_search(chunk, infos_collection)
            if similar_info:
                update_ops = {}
                if chunk.id not in similar_info.chunk_ids:
                    update_ops.setdefault("$addToSet", {})["chunk_ids"] = chunk.id
                if companies_ids:
                    update_ops.setdefault("$addToSet", {})["companiesId"] = {"$each": companies_ids}
                if sources_ids:
                    update_ops.setdefault("$addToSet", {})["sourcesId"] = {"$each": sources_ids}
                if update_ops:
                    update_ops["$set"] = {"last_updated": datetime.now()}
                    try:
                        infos_collection.update_one({"_id": similar_info.id}, update_ops)
                        logger.info(f"Updated info '{similar_info.id}' with chunk '{chunk.id}'")
                    except errors.PyMongoError as e:
                        logger.error(f"MongoDB error updating info '{similar_info.id}': {e}")
            else:
                # Create a new Info document with the current chunk as the head
                new_info = Info(
                    embedding=chunk.embedding,
                    chunk_ids=[chunk.id],
                    companiesId=companies_ids,
                    sourcesId=sources_ids,
                    created_at=datetime.now(),
                    last_updated=datetime.now(),
                )
                try:
                    infos_collection.insert_one(new_info.model_dump(by_alias=True))
                    logger.info(f"Created new info for chunk '{chunk.id}'")
                except errors.PyMongoError as e:
                    logger.error(f"MongoDB error inserting new info for chunk '{chunk.id}': {e}")

            # Mark chunk as processed
            try:
                chunks_collection.update_one({"_id": chunk.id}, {"$set": {"was_processed": True}})
            except errors.PyMongoError as e:
                logger.error(f"Error marking chunk '{chunk.id}' as processed: {e}")

