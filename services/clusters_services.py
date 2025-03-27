from util.mongodb_utils import get_mongo_collection
from env import db_name_stkfeed
import json
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from bson.objectid import ObjectId
import logging
from util.parsing_utils import extract_json_from_content

logger = logging.getLogger(__name__)


def process_clusters():
    """
    Processa clusters não processados aplicando um grafo de análise a cada um,
    de forma sequencial (sem paralelização).
    """
    from util.langchain_utils import connect_to_graph_execution
    
    clusters_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="clusters")
    posts_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="posts")
    graph_coll = get_mongo_collection(db_name="crewai_db", collection_name="graphs")
    
    # Encontre o grafo de análise de cluster
    graph_name = "Alphasync Summarize Cluster"
    analysis_graph = graph_coll.find_one({"name": graph_name})
    if not analysis_graph:
        logger.error("Grafo de análise de cluster não encontrado")
        return
    
    graph_id = str(analysis_graph["_id"])
    
    # Encontre clusters não processados
    unprocessed_clusters = list(clusters_coll.find(
        {"was_processed": False, "label": {"$ne": -1}},
        {"_id": 1, "posts_ids": 1, "label": 1}
    ))
    
    if not unprocessed_clusters:
        logger.info("Não há clusters não processados para analisar")
        return
    
    logger.info(f"Encontrados {len(unprocessed_clusters)} clusters para processar")
    
    # Processa um cluster por vez
    for i, cluster in enumerate(unprocessed_clusters):
        logger.info(f"Processando cluster {i+1}/{len(unprocessed_clusters)}: {cluster['_id']}")
        
        try:
            # Buscar conteúdo completo de todos os posts do cluster
            post_ids = [ObjectId(pid) for pid in cluster["posts_ids"]]
            posts = list(posts_coll.find({"_id": {"$in": post_ids}}))
            
            if not posts:
                logger.warning(f"Cluster {cluster['_id']} não tem posts válidos")
                continue
            
            # Extrair textos dos posts
            cluster_summaries = "\n".join([post.get("content", "") for post in posts])
            
            # Preparar prompt para análise
            initial_message = cluster_summaries
            
            # Executar grafo utilizando connect_to_graph_execution
            logger.info(f"Executando análise do cluster {cluster['_id']}")
            result = asyncio.run(connect_to_graph_execution(
                graph_id=graph_id,
                initial_message=initial_message,
                timeout_seconds=60,
                retry_attempts=3
            ))
            
            # Extrair resumo do resultado
            #extract braces from result
            raw_analysis = extract_json_from_content(result[0]["step"]["Summarize Cluster"][-1]["content"])
            analysis = json.loads(raw_analysis)
            
            logger.info(f"Resumo gerado para cluster {cluster['_id']}: {analysis[:100]}...")
            
            #now lets grab the oldest, the newest and the average age of the posts in hours
            oldest_post = min(posts, key=lambda x: x["created_at"])["created_at"]
            newest_post = max(posts, key=lambda x: x["created_at"])["created_at"]
            average_age = sum((newest_post["created_at"] - post["created_at"]).total_seconds() / 3600 for post in posts) / len(posts)
            [i.get("content") for i in posts]
            clusters_coll.update_one(
                {"_id": cluster["_id"]},
                {"$set": {"summary": analysis, "was_processed": True}}
            )
            
            logger.info(f"Cluster {cluster['_id']} processado com sucesso")
                
        except Exception as e:
            logger.error(f"Erro ao processar cluster {cluster['_id']}: {str(e)}")
    
    logger.info(f"Processamento de clusters concluído. Total: {len(unprocessed_clusters)}")
    
    