from util.mongodb_utils import get_mongo_collection
from env import db_name_alphasync, db_name_stkfeed
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _log_processing_summary(run_start_time: datetime):
    """
    Enhanced logging to include STKFeed metrics
    """
    emails_collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="emails")
    chunks_collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="chunks")
    companies_collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="companies")
    sources_collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="sources")
    infos_collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="infos")
    users_collection = get_mongo_collection(db_name=db_name_stkfeed, collection_name="users")
    posts_collection = get_mongo_collection(db_name=db_name_stkfeed, collection_name="posts")

    # Existing counts
    num_important_chunks = chunks_collection.count_documents({
        "include": True,
        "created_at": {"$gte": run_start_time},
    })
    num_total_chunks = chunks_collection.count_documents({
        "created_at": {"$gte": run_start_time},
    })
    num_relevant_emails = emails_collection.count_documents({
        "relevant": True,
        "received_at": {"$gte": run_start_time},
    })
    num_companies = companies_collection.count_documents({
        "created_at": {"$gte": run_start_time},
    })
    num_sources = sources_collection.count_documents({
        "created_at": {"$gte": run_start_time},
    })
    num_info_docs = infos_collection.count_documents({
        "created_at": {"$gte": run_start_time},
    })
    
    # New STKFeed metrics
    num_users = users_collection.count_documents({
        "created_at": {"$gte": run_start_time},
    })
    num_posts = posts_collection.count_documents({
        "created_at": {"$gte": run_start_time},
    })

    logger.info("----- Processing Summary -----")
    logger.info("[AlphaSync DB]")
    logger.info("Relevant emails: %d", num_relevant_emails)
    logger.info("Important chunks: %d/%d", num_important_chunks, num_total_chunks)
    logger.info("Companies created: %d", num_companies)
    logger.info("Sources created: %d", num_sources)
    logger.info("Info documents: %d", num_info_docs)
    
    logger.info("[STKFeed DB]")
    logger.info("Users created: %d", num_users)
    logger.info("Posts created: %d", num_posts)
    logger.info("-------------------------------")

