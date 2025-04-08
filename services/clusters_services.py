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
from datetime import datetime, timedelta
import statistics
from util.posts_utils import deduplicate_posts
from models.clusters import Cluster
import traceback
import time
import pymongo
import numpy as np
from hdbscan import HDBSCAN

logger = logging.getLogger(__name__)



def obter_posts_com_embeddings(posts_coll, dias=7):
    """
    Obtém os posts com embeddings dos últimos N dias.
    """
    logger.info(f"[CLUSTERING] Buscando posts com embeddings dos últimos {dias} dias")
    
    # Calcular a data de N dias atrás
    dias_atras = datetime.now() - timedelta(days=dias)
    
    documents = list(posts_coll.find(
        {"embedding": {"$exists": True}, "created_at": {"$gte": dias_atras}}, 
        {"embedding": 1, "_id": 1, "title": 1, "content": 1, "created_at": 1}
    ).sort("created_at", -1))
    
    logger.info(f"[CLUSTERING] Encontrados {len(documents)} posts com embeddings nos últimos {dias} dias")
    
    # Verificação inicial de documentos
    if len(documents) == 0:
        logger.warning("[CLUSTERING] Não há documentos com embeddings para clustering")
        return None
    
    # Remover posts duplicados - mesmo título e conteúdo
    logger.info("[CLUSTERING] Removendo posts duplicados")
    unique_documents = deduplicate_posts(documents)
    logger.info(f"[CLUSTERING] De {len(documents)} posts originais, {len(unique_documents)} são únicos por título+conteúdo")
    
    # Verificar se temos posts suficientes para clustering após deduplicação
    if len(unique_documents) < 5:
        logger.warning(f"[CLUSTERING] Apenas {len(unique_documents)} conteúdos únicos para clustering (mínimo 5)")
        return None
    
    return unique_documents, documents


def executar_clustering(unique_documents):
    """
    Executa o clustering HDBSCAN nos documentos e realiza reclustering em clusters grandes.
    """
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

    # Reclustering de clusters grandes (>10% do total)
    continue_clustering = True
    while continue_clustering:
        continue_clustering = False
        # Verificar se ainda existem clusters grandes para reclustering
        for label, count in cluster_counts.items():
            if count > len(unique_documents) * 0.1 and label != -1:
                print(f"[CLUSTERING] Reclusterizando cluster {label} com {count} posts")
                continue_clustering = True
                clusterer = HDBSCAN(min_cluster_size=5, metric="euclidean")
                logger.info(f"[CLUSTERING] Reclusterizando cluster {label} com {count} posts")
                
                # Reclustering do cluster
                max_label = max(labels)
                new_labels = clusterer.fit_predict(embeddings[labels == label])

                # Criar uma máscara para distinguir ruído de clusters válidos
                mask_valid = new_labels != -1
                new_labels_adjusted = np.where(mask_valid, new_labels + max_label, -1)

                # Atualizar os rótulos originais
                labels[labels == label] = new_labels_adjusted
                
                # Atualizar contagem de clusters após reclustering
                cluster_counts = {}
                for label_updated in labels:
                    if label_updated not in cluster_counts:
                        cluster_counts[label_updated] = 0
                    cluster_counts[label_updated] += 1
                
                # Sair do loop for para recomeçar a verificação com os novos clusters
                break
    
    # Log dos resultados do clustering
    noise_count = cluster_counts.get(-1, 0)
    cluster_count = len(cluster_counts) - (1 if -1 in cluster_counts else 0)
    logger.info(f"[CLUSTERING] Resultados HDBSCAN: {cluster_count} clusters encontrados, {noise_count} pontos de ruído")
    
    for label, count in cluster_counts.items():
        if label != -1:
            logger.info(f"[CLUSTERING] Cluster {label}: {count} posts")
    
    # Organizar resultados
    return labels, post_ids, cluster_counts


def organizar_clusters_por_label(labels, post_ids, unique_documents):
    """
    Organiza os posts por clusters, excluindo pontos de ruído.
    Agora também armazena os títulos dos posts para melhor visibilidade nos logs.
    """
    # Criar dicionário para rápido acesso ao título pelo ID
    post_titles = {}
    for doc in unique_documents:
        post_titles[str(doc["_id"])] = doc.get("title", "Sem título")
    
    # Agrupar conteúdos por label
    clusters_by_label = {}
    clusters_titles_by_label = {}  # Novo dicionário para armazenar títulos
    
    for post_id, label in zip(post_ids, labels):
        if label == -1:  # Skip noise points
            continue
            
        if label not in clusters_by_label:
            clusters_by_label[label] = []
            clusters_titles_by_label[label] = []
            
        # Adicionar o post ao cluster
        clusters_by_label[label].append(post_id)
        
        # Adicionar o título ao dicionário de títulos
        title = post_titles.get(post_id, "Título não encontrado")
        clusters_titles_by_label[label].append(title)
    
    # Criar documentos de cluster
    logger.info("[CLUSTERING] Criando documentos de cluster para inserção no MongoDB")
    clusters = []
    for label, post_ids in clusters_by_label.items():
        post_titles = clusters_titles_by_label[label]
        cluster = Cluster(posts_ids=post_ids, label=int(label))
        cluster_data = cluster.model_dump(by_alias=True)
        
        # Adicionar títulos ao objeto do cluster para uso em logs
        cluster_data["post_titles"] = post_titles
        
        clusters.append(cluster_data)
    
    return clusters, clusters_by_label, clusters_titles_by_label


def exportar_log_clusters(clusters, unique_documents, cluster_counts):
    """
    Exporta o log de clusters para um arquivo JSON.
    """
    try:
        clusters_export = []
        for cluster in clusters:
            # Incluir apenas informações essenciais para visualização
            clusters_export.append({
                "label": cluster["label"],
                "posts_count": len(cluster["posts_ids"]),
                "posts_titles": cluster["post_titles"][:10] + (["..."] if len(cluster["post_titles"]) > 10 else [])  # Limitar a exibição
            })
        
        # Criar diretório de logs se não existir
        os.makedirs("logs", exist_ok=True)
        
        # Nome do arquivo com timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"logs/clusters_log_{timestamp}.json"
        
        # Escrever no arquivo JSON
        # with open(log_file, "w", encoding="utf-8") as f:
        #     json.dump({
        #         "timestamp": timestamp,
        #         "total_clusters": len(clusters),
        #         "total_documents": len(unique_documents),
        #         "noise_points": cluster_counts.get(-1, 0),
        #         "clusters": clusters_export
        #     }, f, ensure_ascii=False, indent=2)
        
        logger.info(f"[CLUSTERING] Log de clusters exportado para {log_file}")
        return timestamp
    except Exception as e:
        logger.error(f"[CLUSTERING] Erro ao exportar log de clusters: {str(e)}")
        return datetime.now().strftime("%Y%m%d_%H%M%S")


def verificar_clusters_existentes(clusters, clusters_coll):
    """
    Verifica quais clusters já existem e quais precisam ser atualizados ou inseridos,
    usando agregações do MongoDB para maior eficiência.
    """
    logger.info("[CLUSTERING] Verificando existência de clusters similares antes da inserção")
    
    # Preparar resultado
    clusters_to_insert = []
    clusters_to_update = []
    
    # Processar cada novo cluster
    for new_cluster in clusters:
        new_posts_ids = new_cluster["posts_ids"]
        
        # Primeiro, verificar clusters 100% idênticos (mais rápido)
        identical_cluster = clusters_coll.find_one(
            {"posts_ids": {"$all": new_posts_ids, "$size": len(new_posts_ids)}},
            {"_id": 1}
        )
        
        if identical_cluster:
            logger.info(f"[CLUSTERING] Cluster idêntico encontrado para label {new_cluster['label']}, ignorando inserção")
            continue
        
        # Se não encontrou cluster idêntico, usar agregação para encontrar correspondências parciais
        pipeline = [
            # Estágio 1: Calcular interseção e união para cada cluster existente
            {
                "$project": {
                    "_id": 1,
                    "posts_ids": 1,
                    "intersection": {
                        "$setIntersection": ["$posts_ids", new_posts_ids]
                    }
                }
            },
            # Estágio 2: Calcular o tamanho da interseção
            {
                "$project": {
                    "_id": 1,
                    "posts_ids": 1,
                    "intersection_size": {"$size": "$intersection"},
                    "original_size": {"$size": "$posts_ids"}
                }
            },
            # Estágio 3: Filtrar apenas clusters com pelo menos uma interseção
            {
                "$match": {
                    "intersection_size": {"$gt": 0}
                }
            },
            # Estágio 4: Calcular union_size e match_percentage
            {
                "$addFields": {
                    "union_size": {
                        "$add": [
                            "$original_size",
                            len(new_posts_ids),
                            {"$multiply": [-1, "$intersection_size"]}
                        ]
                    }
                }
            },
            {
                "$addFields": {
                    "match_percentage": {
                        "$multiply": [
                            {"$divide": ["$intersection_size", "$union_size"]},
                            100
                        ]
                    }
                }
            },
            # Estágio 5: Filtrar apenas clusters com correspondência significativa (≥ 40%)
            {
                "$match": {
                    "match_percentage": {"$gte": 40}
                }
            },
            # Estágio 6: Ordenar por percentual de correspondência (decrescente)
            {
                "$sort": {"match_percentage": -1}
            },
            # Estágio 7: Limitar a 1 resultado (melhor correspondência)
            {
                "$limit": 1
            }
        ]
        
        # Executar a agregação
        best_match = list(clusters_coll.aggregate(pipeline))
        
        if best_match:
            # Encontrou uma correspondência significativa
            match = best_match[0]
            match_percentage = match["match_percentage"]
            
            logger.info(f"[CLUSTERING] Cluster com {match_percentage:.2f}% de correspondência encontrado, atualizando")
            
            # Calcular união dos posts_ids para atualização
            new_posts_set = set(new_posts_ids)
            existing_posts_set = set(match["posts_ids"])
            merged_posts_ids = list(new_posts_set.union(existing_posts_set))
            
            clusters_to_update.append({
                "cluster_id": match["_id"],
                "posts_ids": merged_posts_ids,
                "was_processed": False  # Reset do processamento para reanalisar o cluster
            })
        else:
            # Nenhuma correspondência significativa
            logger.info(f"[CLUSTERING] Nenhum cluster similar encontrado para label {new_cluster['label']}, criando novo")
            clusters_to_insert.append(new_cluster)
    
    return clusters_to_insert, clusters_to_update


def atualizar_clusters_existentes(clusters_to_update, clusters_coll):
    """Atualiza os clusters existentes no MongoDB."""
    if clusters_to_update:
        logger.info(f"[CLUSTERING] Atualizando {len(clusters_to_update)} clusters existentes")
        for update_info in clusters_to_update:
            clusters_coll.update_one(
                {"_id": update_info["cluster_id"]},
                {"$set": {
                    "posts_ids": update_info["posts_ids"],
                    "was_processed": False,
                    "was_updated": True  # Marca que o cluster foi atualizado
                }}
            )
    return len(clusters_to_update)


def inserir_novos_clusters(clusters_to_insert, clusters_coll):
    """Insere novos clusters no MongoDB."""
    if clusters_to_insert:
        # Adicionar flag was_updated em cada novo cluster
        for cluster in clusters_to_insert:
            cluster["was_updated"] = False
        # take_out post_titles from clusters_to_insert
        for cluster in clusters_to_insert:
            cluster.pop("post_titles", None)
            
        logger.info(f"[CLUSTERING] Inserindo {len(clusters_to_insert)} novos clusters no MongoDB")
        insert_result = clusters_coll.insert_many(clusters_to_insert)
        logger.info(f"[CLUSTERING] {len(insert_result.inserted_ids)} novos clusters inseridos com sucesso")
        return len(insert_result.inserted_ids)
    else:
        logger.info("[CLUSTERING] Nenhum novo cluster para inserir após verificações de duplicação")
        return 0


def exportar_log_final(clusters, clusters_to_insert, clusters_to_update, timestamp):
    """Exporta o log final de clusters a serem inseridos e atualizados."""
    try:
        # Preparar dados para o log final
        clusters_to_insert_export = []
        for cluster in clusters_to_insert:
            clusters_to_insert_export.append({
                "label": cluster["label"],
                "posts_count": len(cluster["posts_ids"]),
                "posts_titles": cluster["post_titles"][:5] + (["..."] if len(cluster["post_titles"]) > 5 else [])
            })
        
        clusters_to_update_export = []
        for update_info in clusters_to_update:
            # Para clusters a serem atualizados, precisamos encontrar os títulos
            # Encontrar o cluster original com esses IDs
            matching_cluster = None
            for cluster in clusters:
                if set(cluster["posts_ids"]).intersection(set(update_info["posts_ids"])):
                    matching_cluster = cluster
                    break
            
            titles = []
            if matching_cluster:
                # Use os títulos do cluster original ou uma lista vazia se não encontrados
                titles = matching_cluster.get("post_titles", [])[:5]
            
            clusters_to_update_export.append({
                "cluster_id": str(update_info["cluster_id"]),
                "posts_count": len(update_info["posts_ids"]),
                "posts_titles": titles + (["..."] if len(update_info["posts_ids"]) > 5 else [])
            })
        
        # Nome do arquivo com timestamp
        final_log_file = f"logs/clusters_final_{timestamp}.json"
        
        # Escrever no arquivo JSON
        with open(final_log_file, "w", encoding="utf-8") as f:
            json.dump({
                "timestamp": timestamp,
                "total_original_clusters": len(clusters),
                "clusters_to_insert": {
                    "count": len(clusters_to_insert),
                    "details": clusters_to_insert_export
                },
                "clusters_to_update": {
                    "count": len(clusters_to_update),
                    "details": clusters_to_update_export
                }
            }, f, ensure_ascii=False, indent=2)
        
        logger.info(f"[CLUSTERING] Log final de clusters exportado para {final_log_file}")
    except Exception as e:
        logger.error(f"[CLUSTERING] Erro ao exportar log final de clusters: {str(e)}")


def garantir_indices_clusters(clusters_coll):
    """
    Garante que os índices necessários para operações eficientes estejam criados na coleção de clusters.
    """
    try:
        # Criar índice em posts_ids para otimizar verificações de correspondências
        clusters_coll.create_index("posts_ids")
        
        # Criar índice composto para buscar clusters não processados com eficiência
        clusters_coll.create_index([("was_processed", 1), ("label", 1)])
        
        logger.info("[CLUSTERING] Índices verificados/criados na coleção clusters")
    except Exception as e:
        logger.error(f"[CLUSTERING] Erro ao criar índices: {str(e)}")


def clustering_posts():
    """
    Função principal de clustering de posts.
    """
    logger.info("[CLUSTERING] Iniciando função de clustering de posts")
    try:
        # Conexão com as coleções do MongoDB
        posts_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="posts")
        clusters_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="clusters")
        logger.info("[CLUSTERING] Conectado às coleções no MongoDB")
        
        # Garantir que índices necessários existam
        garantir_indices_clusters(clusters_coll)
        
        # Obter e preparar os dados
        result = obter_posts_com_embeddings(posts_coll,dias=7)
        if not result:
            return
        
        unique_documents, original_documents = result
        
        # Executar o clustering
        labels, post_ids, cluster_counts = executar_clustering(unique_documents)
        
        # Organizar os resultados por cluster
        clusters, clusters_by_label, clusters_titles_by_label = organizar_clusters_por_label(labels, post_ids, unique_documents)
        
        # Exportar log inicial de clusters
        # timestamp = exportar_log_clusters(clusters, unique_documents, cluster_counts)
        
        # Verificar clusters existentes
        clusters_to_insert, clusters_to_update = verificar_clusters_existentes(clusters, clusters_coll)
        
        # Atualizar clusters existentes
        num_atualizados = atualizar_clusters_existentes(clusters_to_update, clusters_coll)
        
        # Inserir novos clusters
        num_inseridos = inserir_novos_clusters(clusters_to_insert, clusters_coll)
        
        # Exportar log final
        # exportar_log_final(clusters, clusters_to_insert, clusters_to_update, timestamp)
        
        # Logs informativos finais
        total_posts = sum(len(ids) for ids in clusters_by_label.values() if ids)
        
        logger.info(f"[CLUSTERING] Processados {len(clusters)} clusters candidatos")
        logger.info(f"[CLUSTERING] Resultado final: {num_inseridos} novos clusters, {num_atualizados} atualizados")
        logger.info(f"[CLUSTERING] De {len(original_documents)} posts originais, {len(unique_documents)} são únicos por título+conteúdo")
        logger.info(f"[CLUSTERING] Removidos {len(original_documents) - len(unique_documents)} posts duplicados")
        
    except Exception as e:
        logger.error(f"[CLUSTERING] ERRO CRÍTICO durante o clustering: {str(e)}")
        logger.error(f"[CLUSTERING] Traceback completo: {traceback.format_exc()}")
        raise


#clen all documents from clusters collection
def clean_clusters():
    clusters_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="clusters")
    clusters_coll.delete_many({})
    #delete trends collection
    trends_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="trends")
    trends_coll.delete_many({})
    
    


#process_clusters()
def process_clusters(max_workers=10, model_name="gemini-2.0-flash", max_tokens=100000, timeout=200.0, temperature=1.0):
    """
    Processa clusters não processados aplicando LLM em paralelo,
    enviando todos os prompts de uma vez para o serviço LLM.
    
    Parâmetros:
    - max_workers: Número máximo de workers para processamento paralelo (padrão: 5)
    - model_name: Nome do modelo LLM a ser usado (padrão: "gemini-2.0-flash")
    - max_tokens: Número máximo de tokens na resposta (padrão: 100000)
    - timeout: Tempo máximo de espera em segundos (padrão: 200.0)
    - temperature: Temperatura para geração de respostas (padrão: 1.0)
    
    Abordagem de paralelização:
    1. Prepara todos os prompts em uma única lista
    2. Envia todos os prompts em uma única chamada para execute_with_threads
    3. Processa os resultados e atualiza os clusters no MongoDB
    
    Otimização de consulta:
    - Coleta todos os post_ids de todos os clusters em uma única lista
    - Realiza apenas UMA consulta ao MongoDB para buscar todos os posts de uma vez
    - Distribui os posts encontrados para seus respectivos clusters
    - Reduz drasticamente o número de queries ao banco de dados
    
    Otimização de atualização:
    - Acumula todas as operações de atualização em uma lista
    - Executa todas as atualizações de uma vez usando bulk_write
    - Reduz drasticamente o tempo de atualização no banco de dados
    
    Benefícios:
    - Redução significativa do tempo total de processamento
    - Melhor utilização dos recursos do sistema
    - Processamento mais eficiente de múltiplos clusters
    - Diminui overhead de comunicação com a API do LLM
    - Reduz a carga no banco de dados
    - Minimiza o número de operações no MongoDB
    """
    logger.info(f"[PROCESSO-CLUSTERS] Iniciando processamento de clusters em paralelo (max_workers={max_workers}, model={model_name})")
    
    # Registrar o tempo de início
    start_time = time.time()
    
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
        
        total_clusters = len(unprocessed_clusters)
        logger.info(f"[PROCESSO-CLUSTERS] Encontrados {total_clusters} clusters para processar")
        
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
        
        # Preparar para buscar todos os posts de todos os clusters em uma única consulta
        all_post_ids = []
        post_id_to_cluster_map = {}  # Mapear post_id para cluster_id
        cluster_info_by_id = {}      # Armazenar informações de cada cluster por ID

        # Coletar todos os IDs de posts e criar mapeamentos
        for cluster in unprocessed_clusters:
            cluster_id = cluster["_id"]
            cluster_post_ids = [ObjectId(pid) for pid in cluster["posts_ids"]]
            
            if not cluster_post_ids:
                logger.warning(f"[PROCESSO-CLUSTERS] Cluster {cluster_id} não tem posts")
                continue
                
            # Armazenar informações do cluster
            cluster_info = {
                "cluster_id": cluster_id,
                "post_ids": cluster_post_ids,
                "posts": [],
                "users_ids": set(),
                "post_dates": []
            }
            cluster_info_by_id[cluster_id] = cluster_info
            
            # Adicionar à lista geral e ao mapeamento
            all_post_ids.extend(cluster_post_ids)
            for post_id in cluster_post_ids:
                post_id_to_cluster_map[post_id] = cluster_id
                
        # Eliminar duplicatas (posts podem estar em múltiplos clusters)
        unique_post_ids = list(set(all_post_ids))
        logger.info(f"[PROCESSO-CLUSTERS] Buscando {len(unique_post_ids)} posts únicos para {len(cluster_info_by_id)} clusters")
        
        # Buscar todos os posts de uma vez
        start_query_time = time.time()
        all_posts = list(posts_coll.find({"_id": {"$in": unique_post_ids}}))
        query_time = time.time() - start_query_time
        logger.info(f"[PROCESSO-CLUSTERS] Encontrados {len(all_posts)} posts em {query_time:.2f} segundos")
        
        # Criar índice de posts por ID para acesso rápido
        posts_by_id = {str(post["_id"]): post for post in all_posts}
        
        # Distribuir os posts para seus respectivos clusters
        for post_id_obj, cluster_id in post_id_to_cluster_map.items():
            post_id_str = str(post_id_obj)
            if post_id_str in posts_by_id:
                post = posts_by_id[post_id_str]
                if cluster_id in cluster_info_by_id:
                    # Adicionar o post ao cluster correspondente
                    cluster_info_by_id[cluster_id]["posts"].append(post)
                    
                    # Coletar user ID se existir
                    user_id = post.get("userId")
                    if user_id:
                        cluster_info_by_id[cluster_id]["users_ids"].add(str(user_id))
                        
                    # Processar data do post
                    post_date = post.get("created_at")
                    if post_date:
                        if isinstance(post_date, str):
                            try:
                                post_date = datetime.fromisoformat(post_date.replace('Z', '+00:00'))
                            except ValueError:
                                try:
                                    post_date = datetime.strptime(post_date, "%Y-%m-%dT%H:%M:%S.%fZ")
                                except ValueError:
                                    try:
                                        post_date = datetime.strptime(post_date, "%Y-%m-%d %H:%M:%S")
                                    except ValueError:
                                        logger.warning(f"[PROCESSO-CLUSTERS] Formato de data não reconhecido: {post_date}")
                                        continue
                        cluster_info_by_id[cluster_id]["post_dates"].append(post_date)
        
        # Preparar prompts para todos os clusters válidos
        all_prompts = []
        valid_cluster_data_list = []
        
        for cluster_id, cluster_info in cluster_info_by_id.items():
            posts = cluster_info["posts"]
            
            if not posts:
                logger.warning(f"[PROCESSO-CLUSTERS] Cluster {cluster_id} não tem posts válidos após distribuição")
                continue
                
            logger.info(f"[PROCESSO-CLUSTERS] Preparando prompt para cluster {cluster_id} com {len(posts)} posts")
            
            # Extrair textos dos posts com data no final
            post_contents = [f"{post.get('content', '')} - {post.get('created_at', '')}" for post in posts]
            
            # Preparar dados do cluster para análise (numerado)
            cluster_data = "\n".join([f"\n{i+1}. {post}" for i, post in enumerate(post_contents)])
            
            # Formatar o prompt com dados do cluster
            formatted_prompt = prompt_template.replace("{cluster_data}", cluster_data)
            
            # Adicionar à lista de prompts
            all_prompts.append(formatted_prompt)
            valid_cluster_data_list.append(cluster_info)
        
        if not all_prompts:
            logger.warning("[PROCESSO-CLUSTERS] Não foi possível preparar nenhum prompt válido")
            return
        
        # Executar todos os prompts em paralelo
        logger.info(f"[PROCESSO-CLUSTERS] Enviando {len(all_prompts)} prompts para processamento em paralelo")
        
        raw_responses = execute_with_threads(
            all_prompts,
            model_name=model_name,
            max_tokens=max_tokens,
            timeout=timeout,
            temperature=temperature,
            max_workers=max_workers
        )
        
        logger.info(f"[PROCESSO-CLUSTERS] Recebidas {len(raw_responses)} respostas do LLM")
        
        # Ao final do processamento
        successful_count = 0
        error_count = 0
        
        # Processar os resultados e atualizar os clusters
        for i, (raw_response, cluster_info) in enumerate(zip(raw_responses, valid_cluster_data_list)):
            try:
                cluster_id = cluster_info["cluster_id"]
                logger.info(f"[PROCESSO-CLUSTERS] Processando resposta {i+1}/{len(raw_responses)} para cluster {cluster_id}")
                
                # Extrair JSON da resposta
                extracted_json = extract_json_from_content(raw_response)
                if not extracted_json:
                    logger.error(f"[PROCESSO-CLUSTERS] Não foi possível extrair JSON da resposta para cluster {cluster_id}")
                    error_count += 1
                    continue
                
                logger.info(f"[PROCESSO-CLUSTERS] JSON extraído com sucesso para cluster {cluster_id}")
                analysis = json.loads(extracted_json)
                
                # Calcular estatísticas de datas
                date_info = {}
                post_dates = cluster_info["post_dates"]
                
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
                    "raw_analysis": analysis,
                    "users_ids": list(cluster_info["users_ids"])
                }
                
                # Adicionar informações de datas ao objeto de atualização
                if date_info:
                    update_data.update(date_info)
                
                logger.info(f"[PROCESSO-CLUSTERS] Resumo gerado para cluster {cluster_id}: {update_data['summary'][:100]}...")
                
                # Armazenar a operação de update para execução em lote
                cluster_info["update_data"] = update_data
                
                # Atualizar o contador de sucesso
                successful_count += 1
                
            except Exception as e:
                logger.error(f"[PROCESSO-CLUSTERS] ERRO ao processar resposta para cluster {cluster_info['cluster_id']}: {str(e)}")
                logger.error(traceback.format_exc())
                error_count += 1
        
        # Executar todas as atualizações de uma vez usando bulk_write
        if successful_count > 0:
            start_update_time = time.time()
            logger.info(f"[PROCESSO-CLUSTERS] Preparando atualização em lote para {successful_count} clusters")
            
            # Criar lista de operações de update
            bulk_operations = []
            for cluster_info in valid_cluster_data_list:
                if "update_data" in cluster_info:  # Só incluir clusters que foram processados com sucesso
                    bulk_operations.append(
                        pymongo.UpdateOne(
                            {"_id": cluster_info["cluster_id"]},
                            {"$set": cluster_info["update_data"]}
                        )
                    )
            
            # Executar as operações em lote
            if bulk_operations:
                logger.info(f"[PROCESSO-CLUSTERS] Executando atualização em lote para {len(bulk_operations)} clusters")
                result = clusters_coll.bulk_write(bulk_operations)
                update_time = time.time() - start_update_time
                
                logger.info(f"[PROCESSO-CLUSTERS] Atualização em lote concluída em {update_time:.2f} segundos")
                logger.info(f"[PROCESSO-CLUSTERS] Clusters modificados: {result.modified_count}")
            else:
                logger.warning("[PROCESSO-CLUSTERS] Nenhuma operação de atualização para executar")
        
        # Calcular o tempo total
        end_time = time.time()
        elapsed_time = end_time - start_time
        minutes = int(elapsed_time // 60)
        seconds = elapsed_time % 60
        
        # Registrar estatísticas finais
        logger.info(f"[PROCESSO-CLUSTERS] Processamento em paralelo concluído em {minutes} minutos e {seconds:.2f} segundos")
        logger.info(f"[PROCESSO-CLUSTERS] Total de clusters: {total_clusters}")
        logger.info(f"[PROCESSO-CLUSTERS] Processados com sucesso: {successful_count}")
        logger.info(f"[PROCESSO-CLUSTERS] Erros: {error_count}")
        logger.info(f"[PROCESSO-CLUSTERS] Taxa de sucesso: {(successful_count/total_clusters)*100:.2f}%")
        
        return {
            "total": total_clusters,
            "successful": successful_count,
            "errors": error_count,
            "elapsed_time": elapsed_time
        }
    
    except Exception as e:
        logger.error(f"[PROCESSO-CLUSTERS] ERRO CRÍTICO no processamento de clusters: {str(e)}")
        logger.error(f"[PROCESSO-CLUSTERS] Traceback completo: {traceback.format_exc()}")
        
        # Calcular o tempo mesmo em caso de erro
        end_time = time.time()
        elapsed_time = end_time - start_time
        minutes = int(elapsed_time // 60)
        seconds = elapsed_time % 60
        logger.error(f"[PROCESSO-CLUSTERS] Processo falhou após {minutes} minutos e {seconds:.2f} segundos")
        
        raise
            