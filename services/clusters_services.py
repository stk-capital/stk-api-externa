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

logger = logging.getLogger(__name__)

def process_clusters():
    """
    Processa clusters não processados aplicando LLM diretamente,
    de forma sequencial (sem paralelização).
    """
    clusters_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="clusters")
    posts_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="posts")
    
    # Encontre clusters não processados
    unprocessed_clusters = list(clusters_coll.find(
        {"was_processed": False, "label": {"$ne": -1}},
        {"_id": 1, "posts_ids": 1, "label": 1}
    ))
    
    if not unprocessed_clusters:
        logger.info("Não há clusters não processados para analisar")
        return
    
    logger.info(f"Encontrados {len(unprocessed_clusters)} clusters para processar")
    
    # Carregar o template do prompt
    prompt_path = os.path.join(os.path.dirname(os.path.dirname('__file__')), 'prompts', 'cluster_summarizer.md')
    with open(prompt_path, 'r', encoding='utf-8') as file:
        prompt_template = file.read()
    
    # Processa um cluster por vez
    for i, cluster in enumerate(unprocessed_clusters):
        #i=9
        #cluster = unprocessed_clusters[i]
        logger.info(f"Processando cluster {i+1}/{len(unprocessed_clusters)}: {cluster['_id']}")
        
        try:
            # Buscar conteúdo completo de todos os posts do cluster
            post_ids = [ObjectId(pid) for pid in cluster["posts_ids"]]
            posts = list(posts_coll.find({"_id": {"$in": post_ids}}))
            
            if not posts:
                logger.warning(f"Cluster {cluster['_id']} não tem posts válidos")
                continue
            
            # Coletar IDs dos usuários sem duplicatas
            users_ids = set()
            for post in posts:
                user_id = post.get("userId")
                if user_id:
                    users_ids.add(str(user_id))
            
            # Converter o conjunto para lista para armazenamento no MongoDB
            users_ids_list = list(users_ids)
            logger.info(f"Cluster com {len(users_ids_list)} usuários únicos")
            
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
                                    logger.warning(f"Formato de data não reconhecido: {post_date}")
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
                
                logger.info(f"Faixa de datas do cluster: {oldest_date.strftime('%Y-%m-%d')} a {newest_date.strftime('%Y-%m-%d')} ({date_info['date_range_days']} dias)")
            
            # Extrair textos dos posts, also add date at the end of the post
            #post_contents = [post.get("content", "") for post in posts]
            post_contents = [f"{post.get('content', '')} - {post.get('created_at', '')}" for post in posts]
            
            # Preparar dados do cluster para análise (numarated list)
            cluster_data = "\n".join([f"\n{i+1}. {post}" for i, post in enumerate(post_contents)])
            #cluster_data = "\n".join(post_contents)
            
            
            
            # Formatar o prompt com dados do cluster
            
            formatted_prompt = prompt_template.replace("{cluster_data}", cluster_data)
            
            # Executar análise usando a função LLM diretamente
            logger.info(f"Executando análise do cluster {cluster['_id']} usando serviço LLM direto")
            #add the formatted prompt to .md file with a nice format
            with open("logs/formatted_prompt.md", "w") as file:
                file.write(formatted_prompt)
            
            # Usar execute_with_threads (versão síncrona)
            raw_response = execute_with_threads(
                formatted_prompt,
                model_name="gemini-2.0-flash",  # Ou outro modelo configurado
                max_tokens=4000,
                timeout=120.0,
                temperature=1
            )
            #add to a .md file with a nice format
            with open("logs/raw_response.md", "w") as file:
                file.write(raw_response)
            # Processar resposta (converter string JSON para objeto)
            try:
                # Primeiro tenta extrair JSON da resposta
                extracted_json = extract_json_from_content(raw_response)
                if extracted_json:
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
                    
                    logger.info(f"Resumo gerado para cluster {cluster['_id']}: {update_data['summary'][:100]}...")
                    logger.info(f"Pontuação de dispersão: {update_data['dispersion_score']}")
                    
                    # Atualizar o cluster no banco de dados com todos os campos
                    clusters_coll.update_one(
                        {"_id": cluster["_id"]},
                        {"$set": update_data}
                    )
                else:
                    # Caso não seja JSON, mantém como texto
                    logger.warning(f"Resposta não é um JSON válido: {raw_response[:200]}...")
                    update_data = {"summary": raw_response, "was_processed": True}
                    
                    # Adicionar informações de datas mesmo em caso de falha do JSON
                    if date_info:
                        update_data.update(date_info)
                    
                    # Adicionar lista de IDs de usuários
                    update_data["users_ids"] = users_ids_list
                    
                    clusters_coll.update_one(
                        {"_id": cluster["_id"]},
                        {"$set": update_data}
                    )
            except json.JSONDecodeError as e:
                logger.warning(f"Erro ao processar JSON da resposta: {str(e)}")
                update_data = {"summary": raw_response, "was_processed": True}
                if date_info:
                    update_data.update(date_info)
                
                # Adicionar lista de IDs de usuários
                update_data["users_ids"] = users_ids_list
                
                clusters_coll.update_one(
                    {"_id": cluster["_id"]},
                    {"$set": update_data}
                )
            except Exception as e:
                logger.warning(f"Erro ao processar resposta: {str(e)}")
                update_data = {"summary": raw_response, "was_processed": True}
                if date_info:
                    update_data.update(date_info)
                
                # Adicionar lista de IDs de usuários
                update_data["users_ids"] = users_ids_list
                
                clusters_coll.update_one(
                    {"_id": cluster["_id"]},
                    {"$set": update_data}
                )
            
            logger.info(f"Cluster {cluster['_id']} processado com sucesso")
                
        except Exception as e:
            logger.error(f"Erro ao processar cluster {cluster['_id']}: {str(e)}")
    
    logger.info(f"Processamento de clusters concluído. Total: {len(unprocessed_clusters)}")
    
            