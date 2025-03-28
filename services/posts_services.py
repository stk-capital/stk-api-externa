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

class Cluster(BaseModel):
    id: str = Field(default_factory=uuid_str, alias="_id")
    posts_ids: List[str]
    summary: str = Field(default="")
    theme: str = Field(default="")
    relevance_score: float = Field(default=0.0)
    label: int = Field(default=-1)
    was_processed: bool = Field(default=False)
    created_at: datetime = Field(default_factory=datetime.now)

    def add_post(self, post_id):
        self.posts_ids.append(post_id)

    def get_summary(self):
        return "\n".join([post.summary for post in self.posts])


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


def deduplicate_posts(posts):
    """
    Remove posts duplicados (mesmo título e conteúdo).
    
    Args:
        posts: Lista de posts
        
    Returns:
        Lista de posts únicos (mantendo o primeiro de cada grupo de posts idênticos)
    """
    # Dicionário para mapear a combinação title+content para o primeiro post com essa combinação
    unique_posts_map = {}
    
    # Estatísticas para logging
    total_posts = len(posts)
    duplicates_found = 0
    
    for post in posts:
        # Criar uma chave baseada no título e conteúdo
        title = post.get('title', '')
        content = post.get('content', '')
        
        # Se não tiver título, usa apenas o conteúdo
        if not title:
            key = content
        else:
            key = f"{title}:{content}"
        
        # Se já existe um post com esta combinação, é uma duplicata
        if key in unique_posts_map:
            duplicates_found += 1
            # Podemos registrar informações sobre duplicatas encontradas
            logger.debug(f"Post duplicado encontrado: {post.get('_id')} (original: {unique_posts_map[key].get('_id')})")
        else:
            # Se é o primeiro post com esta combinação, adiciona ao mapa
            unique_posts_map[key] = post
    
    # Converter o mapa de volta para uma lista
    unique_posts = list(unique_posts_map.values())
    
    # Mostrar estatísticas
    if duplicates_found > 0:
        logger.info(f"Encontrados {duplicates_found} posts duplicados de um total de {total_posts}.")
    
    return unique_posts


def clustering_posts():
    #
    posts_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="posts")
    clusters_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="clusters")
    
    # Limpar clusters existentes
    clusters_coll.delete_many({})
    
    from hdbscan import HDBSCAN
    import numpy as np
    
    # Buscar documentos com embeddings e infoId
    documents = list(posts_coll.find(
        {"embedding": {"$exists": True}}, 
        {"embedding": 1, "_id": 1, "title": 1, "content": 1}
    ))
    
    # Verificação inicial de documentos
    if len(documents) == 0:
        logger.info("Não há documentos com embeddings para clustering")
        return
    
    # Remover posts duplicados - mesmo título e conteúdo
    unique_documents = deduplicate_posts(documents)
    logger.info(f"De {len(documents)} posts originais, {len(unique_documents)} são únicos por título+conteúdo")
    
    # Verificar se temos posts suficientes para clustering após deduplicação
    if len(unique_documents) < 5:
        logger.info(f"Apenas {len(unique_documents)} conteúdos únicos para clustering (mínimo 5)")
        return
    
    # Preparar arrays para clustering diretamente com os documentos únicos
    embeddings = np.array([doc["embedding"] for doc in unique_documents])
    post_ids = [str(doc["_id"]) for doc in unique_documents]
    
    # Cluster com HDBSCAN
    clusterer = HDBSCAN(min_cluster_size=5, metric="euclidean")
    labels = clusterer.fit_predict(embeddings)
    
    # Agrupar conteúdos por label
    clusters_by_label = {}
    for post_id, label in zip(post_ids, labels):
        if label == -1:  # Skip noise points
            continue
            
        if label not in clusters_by_label:
            clusters_by_label[label] = []
            
        # Adicionar o post ao cluster
        clusters_by_label[label].append(post_id)
    
    # Criar documentos de cluster
    clusters = []
    for label, post_ids in clusters_by_label.items():
        cluster = Cluster(posts_ids=post_ids, label=int(label))
        clusters.append(cluster.model_dump(by_alias=True))
    
    # Inserir clusters no MongoDB
    if clusters:
        clusters_coll.insert_many(clusters)
        
        # Logs informativos
        total_posts = sum(len(ids) for ids in clusters_by_label.values() if ids)
        
        logger.info(f"Criados {len(clusters)} clusters agrupando {total_posts} posts")
        logger.info(f"De {len(documents)} posts originais, {len(unique_documents)} são únicos por título+conteúdo")
        logger.info(f"Removidos {len(documents) - len(unique_documents)} posts duplicados")

#clen all documents from clusters collection
def clean_clusters():
    clusters_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="clusters")
    clusters_coll.delete_many({})


def _process_posts():
    create_posts_from_infos()