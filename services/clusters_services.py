from util.mongodb_utils import get_mongo_collection
from env import db_name_stkfeed
import json
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from bson.objectid import ObjectId
import logging
from util.parsing_utils import extract_json_from_content
import os
from util.llm_services import execute_with_threads, execute_async
from datetime import datetime
import statistics
from util.posts_utils import deduplicate_posts
from models.clusters import Cluster
import traceback

logger = logging.getLogger(__name__)


def clustering_posts():
    #
    logger.info("[CLUSTERING] Iniciando função de clustering de posts")
    try:
        posts_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="posts")
        clusters_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="clusters")
        
        logger.info("[CLUSTERING] Conectado às coleções no MongoDB")
        
        # Limpar clusters existentes
        logger.info("[CLUSTERING] Limpando clusters existentes")
        delete_result = clusters_coll.delete_many({})
        logger.info(f"[CLUSTERING] {delete_result.deleted_count} clusters anteriores foram removidos")
        
        from hdbscan import HDBSCAN
        import numpy as np
        
        # Buscar documentos com embeddings e infoId
        logger.info("[CLUSTERING] Buscando posts com embeddings")
        documents = list(posts_coll.find(
            {"embedding": {"$exists": True}}, 
            {"embedding": 1, "_id": 1, "title": 1, "content": 1}
        ).sort("created_at", -1))
        
        logger.info(f"[CLUSTERING] Encontrados {len(documents)} posts com embeddings")
        
        # Verificação inicial de documentos
        if len(documents) == 0:
            logger.warning("[CLUSTERING] Não há documentos com embeddings para clustering")
            return
        
        # Remover posts duplicados - mesmo título e conteúdo
        logger.info("[CLUSTERING] Removendo posts duplicados")
        unique_documents = deduplicate_posts(documents)
        logger.info(f"[CLUSTERING] De {len(documents)} posts originais, {len(unique_documents)} são únicos por título+conteúdo")
        
        # Verificar se temos posts suficientes para clustering após deduplicação
        if len(unique_documents) < 5:
            logger.warning(f"[CLUSTERING] Apenas {len(unique_documents)} conteúdos únicos para clustering (mínimo 5)")
            return
        
        # Preparar arrays para clustering diretamente com os documentos únicos
        logger.info("[CLUSTERING] Preparando arrays de embeddings para HDBSCAN")
        embeddings = np.array([doc["embedding"] for doc in unique_documents])
        post_ids = [str(doc["_id"]) for doc in unique_documents]
        
        logger.info(f"[CLUSTERING] Iniciando HDBSCAN com {len(embeddings)} embeddings")
        # Cluster com HDBSCAN
        clusterer = HDBSCAN(min_cluster_size=5, metric="euclidean")
        labels = clusterer.fit_predict(embeddings)
        logger.info(f"[CLUSTERING] HDBSCAN concluído, processando resultados")
        
        # Contar o número de pontos por cluster
        cluster_counts = {}
        for label in labels:
            if label not in cluster_counts:
                cluster_counts[label] = 0
            cluster_counts[label] += 1
            
        # Log dos resultados do clustering
        noise_count = cluster_counts.get(-1, 0)
        cluster_count = len(cluster_counts) - (1 if -1 in cluster_counts else 0)
        logger.info(f"[CLUSTERING] Resultados HDBSCAN: {cluster_count} clusters encontrados, {noise_count} pontos de ruído")
        
        for label, count in cluster_counts.items():
            if label != -1:
                logger.info(f"[CLUSTERING] Cluster {label}: {count} posts")
        
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
        logger.info("[CLUSTERING] Criando documentos de cluster para inserção no MongoDB")
        clusters = []
        for label, post_ids in clusters_by_label.items():
            cluster = Cluster(posts_ids=post_ids, label=int(label))
            clusters.append(cluster.model_dump(by_alias=True))
        
        # Inserir clusters no MongoDB
        if clusters:
            logger.info(f"[CLUSTERING] Inserindo {len(clusters)} clusters no MongoDB")
            insert_result = clusters_coll.insert_many(clusters)
            logger.info(f"[CLUSTERING] {len(insert_result.inserted_ids)} clusters inseridos com sucesso")
            
            # Logs informativos
            total_posts = sum(len(ids) for ids in clusters_by_label.values() if ids)
            
            logger.info(f"[CLUSTERING] Criados {len(clusters)} clusters agrupando {total_posts} posts")
            logger.info(f"[CLUSTERING] De {len(documents)} posts originais, {len(unique_documents)} são únicos por título+conteúdo")
            logger.info(f"[CLUSTERING] Removidos {len(documents) - len(unique_documents)} posts duplicados")
        else:
            logger.warning("[CLUSTERING] Nenhum cluster foi criado após o processamento")
    except Exception as e:
        logger.error(f"[CLUSTERING] ERRO CRÍTICO durante o clustering: {str(e)}")
        logger.error(f"[CLUSTERING] Traceback completo: {traceback.format_exc()}")
        raise

#clen all documents from clusters collection
def clean_clusters():
    clusters_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="clusters")
    clusters_coll.delete_many({})

def process_clusters():
    """
    Processa clusters não processados aplicando LLM diretamente,
    de forma sequencial (sem paralelização).
    """
    logger.info("[PROCESSO-CLUSTERS] Iniciando processamento de clusters")
    try:
        clusters_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="clusters")
        posts_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="posts")
        
        logger.info("[PROCESSO-CLUSTERS] Conectado às coleções no MongoDB")
        
        # Encontre clusters não processados
        logger.info("[PROCESSO-CLUSTERS] Buscando clusters não processados")
        unprocessed_clusters = list(clusters_coll.find(
            {"was_processed": False, "label": {"$ne": -1}},
            {"_id": 1, "posts_ids": 1, "label": 1}
        ))
        
        if not unprocessed_clusters:
            logger.info("[PROCESSO-CLUSTERS] Não há clusters não processados para analisar")
            return
        
        logger.info(f"[PROCESSO-CLUSTERS] Encontrados {len(unprocessed_clusters)} clusters para processar")
        
        # Carregar o template do prompt
        prompt_path = os.path.join(os.path.dirname(os.path.dirname('__file__')), 'prompts', 'cluster_summarizer.md')
        try:
            with open(prompt_path, 'r', encoding='utf-8') as file:
                prompt_template = file.read()
            logger.info(f"[PROCESSO-CLUSTERS] Template de prompt carregado de {prompt_path}")
        except Exception as e:
            logger.error(f"[PROCESSO-CLUSTERS] ERRO ao carregar prompt template: {str(e)}")
            logger.error(f"[PROCESSO-CLUSTERS] Caminho do prompt: {prompt_path}")
            raise
        
        # Processa um cluster por vez
        for i, cluster in enumerate(unprocessed_clusters):
            #i=9
            #cluster = unprocessed_clusters[i]
            logger.info(f"[PROCESSO-CLUSTERS] Processando cluster {i+1}/{len(unprocessed_clusters)}: {cluster['_id']}")
            
            try:
                # Buscar conteúdo completo de todos os posts do cluster
                post_ids = [ObjectId(pid) for pid in cluster["posts_ids"]]
                logger.info(f"[PROCESSO-CLUSTERS] Buscando {len(post_ids)} posts para o cluster {cluster['_id']}")
                posts = list(posts_coll.find({"_id": {"$in": post_ids}}))
                
                if not posts:
                    logger.warning(f"[PROCESSO-CLUSTERS] Cluster {cluster['_id']} não tem posts válidos")
                    continue
                
                logger.info(f"[PROCESSO-CLUSTERS] Encontrados {len(posts)} posts válidos de {len(post_ids)} IDs no cluster")
                
                # Coletar IDs dos usuários sem duplicatas
                users_ids = set()
                for post in posts:
                    user_id = post.get("userId")
                    if user_id:
                        users_ids.add(str(user_id))
                
                # Converter o conjunto para lista para armazenamento no MongoDB
                users_ids_list = list(users_ids)
                logger.info(f"[PROCESSO-CLUSTERS] Cluster com {len(users_ids_list)} usuários únicos")
                
                # Processamento de datas
                post_dates = []
                for post in posts:
                    post_date = post.get("created_at")
                    if post_date:
                        if isinstance(post_date, str):
                            try:
                                # Tentar converter string para datetime
                                post_date = datetime.fromisoformat(post_date.replace('Z', '+00:00'))
                            except ValueError:
                                try:
                                    # Tentar outros formatos comuns
                                    post_date = datetime.strptime(post_date, "%Y-%m-%dT%H:%M:%S.%fZ")
                                except ValueError:
                                    try:
                                        post_date = datetime.strptime(post_date, "%Y-%m-%d %H:%M:%S")
                                    except ValueError:
                                        logger.warning(f"[PROCESSO-CLUSTERS] Formato de data não reconhecido: {post_date}")
                                        continue
                        post_dates.append(post_date)
                
                # Calcular estatísticas de datas
                date_info = {}
                if post_dates:
                    oldest_date = min(post_dates)
                    newest_date = max(post_dates)
                    
                    # Converter para timestamps para calcular média
                    timestamps = [date.timestamp() for date in post_dates]
                    avg_timestamp = statistics.mean(timestamps)
                    avg_date = datetime.fromtimestamp(avg_timestamp)
                    
                    date_info = {
                        "oldest_post_date": oldest_date,
                        "newest_post_date": newest_date,
                        "average_post_date": avg_date,
                        "date_range_days": (newest_date - oldest_date).days
                    }
                    
                    logger.info(f"[PROCESSO-CLUSTERS] Faixa de datas do cluster: {oldest_date.strftime('%Y-%m-%d')} a {newest_date.strftime('%Y-%m-%d')} ({date_info['date_range_days']} dias)")
                
                # Extrair textos dos posts, also add date at the end of the post
                #post_contents = [post.get("content", "") for post in posts]
                post_contents = [f"{post.get('content', '')} - {post.get('created_at', '')}" for post in posts]
                
                # Preparar dados do cluster para análise (numarated list)
                cluster_data = "\n".join([f"\n{i+1}. {post}" for i, post in enumerate(post_contents)])
                #cluster_data = "\n".join(post_contents)
                
                logger.info(f"[PROCESSO-CLUSTERS] Preparados {len(post_contents)} posts para análise com LLM")
                
                # Formatar o prompt com dados do cluster
                formatted_prompt = prompt_template.replace("{cluster_data}", cluster_data)
                
                # Executar análise usando a função LLM diretamente
                logger.info(f"[PROCESSO-CLUSTERS] Executando análise do cluster {cluster['_id']} usando serviço LLM direto")
                  
                # Usar execute_with_threads (versão síncrona)
                logger.info(f"[PROCESSO-CLUSTERS] Chamando LLM (gemini-2.0-flash) para cluster {cluster['_id']}")
                raw_response = execute_with_threads(
                    formatted_prompt,
                    model_name="gemini-2.0-flash",  # Ou outro modelo configurado
                    max_tokens=4000,
                    timeout=120.0,
                    temperature=1
                )
                logger.info(f"[PROCESSO-CLUSTERS] Resposta do LLM recebida para cluster {cluster['_id']}")
                
                # Salvar resposta para depuraç
                
                # Processar resposta (converter string JSON para objeto)
                try:
                    # Primeiro tenta extrair JSON da resposta
                    logger.info(f"[PROCESSO-CLUSTERS] Extraindo JSON da resposta para cluster {cluster['_id']}")
                    extracted_json = extract_json_from_content(raw_response)
                    if extracted_json:
                        logger.info(f"[PROCESSO-CLUSTERS] JSON extraído com sucesso para cluster {cluster['_id']}")
                        analysis = json.loads(extracted_json)
                        
                        # Preparar objeto de atualização com todos os campos do resultado
                        update_data = {
                            "was_processed": True,
                            "summary": analysis.get("summary", ""),
                            "theme": analysis.get("theme", ""),
                            "key_points": analysis.get("key_points", []),
                            "relevance_score": analysis.get("relevance_score", 0.0),
                            "dispersion_score": analysis.get("dispersion_score", 0.0),
                            "stakeholder_impact": analysis.get("stakeholder_impact", ""),
                            "sector_specific": {
                                "opportunities": analysis.get("sector_specific", {}).get("opportunities", []),
                                "risks": analysis.get("sector_specific", {}).get("risks", [])
                            },
                            "raw_analysis": analysis,  # Salvar o objeto completo para referência
                            "users_ids": users_ids_list
                        }
                        
                        # Adicionar informações de datas ao objeto de atualização
                        if date_info:
                            update_data.update(date_info)
                        
                        logger.info(f"[PROCESSO-CLUSTERS] Resumo gerado para cluster {cluster['_id']}: {update_data['summary'][:100]}...")
                        logger.info(f"[PROCESSO-CLUSTERS] Pontuação de dispersão: {update_data['dispersion_score']}")
                        
                        # Atualizar o cluster no banco de dados com todos os campos
                        logger.info(f"[PROCESSO-CLUSTERS] Atualizando cluster {cluster['_id']} no MongoDB")
                        update_result = clusters_coll.update_one(
                            {"_id": cluster["_id"]},
                            {"$set": update_data}
                        )
                        logger.info(f"[PROCESSO-CLUSTERS] Cluster {cluster['_id']} atualizado: {update_result.modified_count} documentos modificados")
                    else:
                        logger.error(f"[PROCESSO-CLUSTERS] Não foi possível extrair JSON da resposta do LLM para cluster {cluster['_id']}")
                except Exception as e:
                    logger.error(f"[PROCESSO-CLUSTERS] ERRO ao processar resposta do LLM para cluster {cluster['_id']}: {str(e)}")
                    logger.error(traceback.format_exc())
            
            except Exception as e:
                logger.error(f"[PROCESSO-CLUSTERS] ERRO ao processar cluster {cluster['_id']}: {str(e)}")
                logger.error(f"[PROCESSO-CLUSTERS] Traceback: {traceback.format_exc()}")
    
    except Exception as e:
        logger.error(f"[PROCESSO-CLUSTERS] ERRO CRÍTICO no processamento de clusters: {str(e)}")
        logger.error(f"[PROCESSO-CLUSTERS] Traceback completo: {traceback.format_exc()}")
        raise
            