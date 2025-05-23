from util.mongodb_utils import get_mongo_collection
from env import db_name_alphasync, db_name_stkfeed
from models.posts import Post
from util.embedding_utils import get_embedding
from util.dates_utils import relative_time
from util.outlook_utils import send_notification_email
from pymongo import errors
import logging
from concurrent.futures import ThreadPoolExecutor
import threading
import uuid
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from pydantic import BaseModel, Field
from typing import List
from datetime import datetime
def uuid_str():
    return str(uuid.uuid4())


def create_posts_from_infos():
    """Create STKFeed posts for new infos in alphasync_db"""
    from bson import ObjectId  # Add explicit import for ObjectId
    
    infos_coll = get_mongo_collection(db_name=db_name_alphasync, collection_name="infos")
    posts_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="posts")
    users_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="users")
    sources_coll = get_mongo_collection(db_name=db_name_alphasync, collection_name="sources")
    chunks_coll = get_mongo_collection(db_name=db_name_alphasync, collection_name="chunks")
    
    # Para controle de novos posts criados
    new_posts_created = []
    
    # Create a unique index to prevent duplicate posts
    try:
        posts_coll.create_index([("infoId", 1), ("userId", 1)], unique=True)
        logger.info("Ensured unique index on posts collection")
    except Exception as e:
        logger.error(f"Error creating index: {e}")
    
    # Get all existing post infoIds to use in our filter
    existing_info_ids = set()
    for post in posts_coll.find({}, {"infoId": 1}):
        if "infoId" in post:
            existing_info_ids.add(post["infoId"])
    
    # Query to find infos that don't have posts yet and have at least one chunk ID
    query = {
        "_id": {"$nin": [ObjectId(id_str) for id_str in existing_info_ids if ObjectId.is_valid(id_str)]},
        "chunk_ids": {"$exists": True, "$ne": []}
    }
    
    # Process only infos that don't have posts yet
    for info in infos_coll.find(query):
        try:
            # Convert ObjectId to string
            info_id_str = str(info['_id'])
            
            # Get first chunk content for post body - fetch in batch first
            if not info.get('chunk_ids') or len(info['chunk_ids']) == 0:
                logger.warning(f"No chunk IDs found for info {info_id_str}")
                continue
                
            chunk = chunks_coll.find_one({"_id": info['chunk_ids'][0]})
            if not chunk:
                logger.warning(f"Chunk not found for info {info_id_str}")
                continue
            
            # Get source name
            source_name = "Unknown"
            if info.get('sourcesId') and len(info['sourcesId']) > 0:
                try:
                    source_doc = sources_coll.find_one({"_id": info['sourcesId'][0]})
                    source_name = source_doc.get('name', 'Unknown') if source_doc else 'Unknown'
                except Exception as e:
                    logger.error(f"Error fetching source: {e}")
            
            # Get associated companies and their users
            company_ids = [str(company_id) for company_id in info.get('companiesId', [])]
            if not company_ids:
                logger.warning(f"No company IDs found for info {info_id_str}")
                continue
            
            # Find all users for these companies in a single query
            company_users = list(users_coll.find({"companyId": {"$in": company_ids}}))
            if not company_users:
                logger.info(f"No users found for companies in info {info_id_str}")
                continue
            
            # Create a batch of posts to insert
            posts_to_insert = []
            post_data_list = []
            
            for user in company_users:
                user_id_str = str(user['_id'])
                
                # Check if this specific user already has a post for this info
                existing_user_post = posts_coll.find_one({
                    "infoId": info_id_str,
                    "userId": user_id_str
                })
                
                if existing_user_post:
                    logger.info(f"Post already exists for info {info_id_str} and user {user_id_str}")
                    continue
                
                post_title = chunk.get('subject', '')  # Use chunk subject if available
                
                # Trim whitespace from the beginning of chunk content
                chunk_content = chunk.get('content', '').lstrip() if chunk.get('content') else ''
                chunk_summary = chunk.get('summary', '').lstrip() if chunk.get('summary') else ''
                
                post = Post(
                    infoId=info_id_str,
                    userId=user_id_str,
                    source=source_name,
                    title=post_title if post_title else "Industry Update",
                    embedding=get_embedding(chunk_summary),
                    content = f"{chunk_summary}: \n\n ´´´{chunk_content}´´´" 
                    if chunk_content else "Industry update",
                    timestamp=relative_time(info['created_at']),
                    created_at=info['created_at']
                )
                
                # Add created_at from info to prevent duplicate timing issues
                post.created_at = info['created_at']
                
                post_dict = post.model_dump(by_alias=True)
                posts_to_insert.append(post_dict)
                post_data_list.append(post_dict.copy())
            
            # Bulk insert posts if there are any
            if posts_to_insert:
                try:
                    result = posts_coll.insert_many(posts_to_insert, ordered=False)
                    logger.info(f"Created {len(result.inserted_ids)} posts for info {info_id_str}")
                    
                    # Add IDs to post data and append to new_posts_created
                    for i, post_id in enumerate(result.inserted_ids):
                        post_data_list[i]['_id'] = str(post_id)
                        new_posts_created.append(post_data_list[i])
                        
                except errors.BulkWriteError as bwe:
                    # Handle partial successes
                    successful_inserts = len(posts_to_insert) - len(bwe.details['writeErrors'])
                    logger.warning(f"Bulk insert partially successful: {successful_inserts}/{len(posts_to_insert)} posts created")
                    
                    # If there were any successful inserts, process them
                    if 'insertedIds' in bwe.details:
                        for idx, post_id in bwe.details['insertedIds'].items():
                            idx = int(idx)  # MongoDB returns indices as strings
                            post_data_list[idx]['_id'] = str(post_id)
                            new_posts_created.append(post_data_list[idx])
                            
                except Exception as e:
                    logger.error(f"Error bulk creating posts: {e}")
                
        except Exception as e:
            logger.error(f"Failed processing info {info.get('_id')}: {e}")
    
    # Envia email de notificação se novos posts foram criados
    if new_posts_created:
        logger.info(f"Enviando email de notificação para {len(new_posts_created)} novos posts criados")
        send_notification_email(new_posts_created)
    
    return len(new_posts_created)

#add embedding to posts that don't have it, in this case it will be the related infos first chunk summary
def add_embedding_to_posts(max_workers=10):
    #db_name_stkfeed = "STKFeed"
    #db_name_alphasync = "alphasync_db"
    posts_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="posts")
    infos_coll = get_mongo_collection(db_name=db_name_alphasync, collection_name="infos")
    chunks_coll = get_mongo_collection(db_name=db_name_alphasync, collection_name="chunks")
    
    posts_without_embedding = list(posts_coll.find({"embedding": {"$exists": False}}))
    status = {"total": len(posts_without_embedding), "processed": 0}
    print_lock = threading.Lock()
    
    def process_post(post):
        info = infos_coll.find_one({"_id": post["infoId"]})
        
        if info:
            chunk_id = info.get("chunk_ids", [{}])[0]
            chunk = chunks_coll.find_one({"_id": chunk_id})
            summary = chunk.get("summary", "")
            embedding = get_embedding(summary)
        else:
            embedding = get_embedding(post["content"])
        
        posts_coll.update_one(
            {"_id": post["_id"]},
            {"$set": {"embedding": embedding}}
        )
        
        with print_lock:
            status["processed"] += 1
            logger.info(f"Processed {status['processed']} of {status['total']} posts")
    max_workers = 10
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(process_post, posts_without_embedding)




def _process_posts():
    create_posts_from_infos()