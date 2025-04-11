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
from util.embedding_utils import get_embedding

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
    # Cluster com HDBSCAN - sem o parâmetro store_centers que não é suportado
    clusterer = HDBSCAN(min_cluster_size=5, metric="euclidean")
    labels = clusterer.fit_predict(embeddings)
    logger.info(f"[CLUSTERING] HDBSCAN concluído, processando resultados")
    
    # Calcular centroides manualmente para cada cluster
    centroids = {}
    for label in set(labels):
        if label != -1:  # Ignorar pontos de ruído (outliers)
            # Encontrar todos os pontos pertencentes a este cluster
            cluster_mask = (labels == label)
            # Calcular o centroide como a média dos embeddings do cluster
            cluster_embeddings = embeddings[cluster_mask]
            centroid = np.mean(cluster_embeddings, axis=0)
            centroids[label] = centroid.tolist()  # Converter para lista para serialização JSON
    
    logger.info(f"[CLUSTERING] Calculados {len(centroids)} centroides de clusters manualmente")
    
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
                # Subclustering sem o parâmetro store_centers
                subclustering = HDBSCAN(min_cluster_size=5, metric="euclidean")
                logger.info(f"[CLUSTERING] Reclusterizando cluster {label} com {count} posts")
                
                # Encontrar embeddings para este cluster
                cluster_mask = (labels == label)
                subcluster_embeddings = embeddings[cluster_mask]
                
                # Reclustering do cluster
                max_label = max(labels)
                new_labels = subclustering.fit_predict(subcluster_embeddings)

                # Criar uma máscara para distinguir ruído de clusters válidos
                mask_valid = new_labels != -1
                new_labels_adjusted = np.where(mask_valid, new_labels + max_label, -1)

                # Atualizar os rótulos originais
                labels[cluster_mask] = new_labels_adjusted
                
                # Calcular centroides dos novos subclusters manualmente
                for sublabel in set(new_labels):
                    if sublabel != -1:  # Ignorar pontos de ruído
                        new_label = sublabel + max_label
                        # Encontrar embeddings para este subcluster
                        submask = (new_labels == sublabel)
                        subcluster_points = subcluster_embeddings[submask]
                        # Calcular centroide
                        subcentroid = np.mean(subcluster_points, axis=0)
                        centroids[new_label] = subcentroid.tolist()
                
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
    return labels, post_ids, cluster_counts, centroids


def organizar_clusters_por_label(labels, post_ids, unique_documents, centroids=None):
    """
    Organiza os posts por clusters, excluindo pontos de ruído.
    Agora também armazena os títulos dos posts para melhor visibilidade nos logs
    e os centroides dos clusters para análise posterior.
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
        
        # Adicionar centroide ao objeto do cluster se disponível
        if centroids and int(label) in centroids:
            cluster_data["embedding"] = centroids[int(label)]
            logger.debug(f"[CLUSTERING] Adicionado centroide para cluster {label}")
        
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

def find_similar_clusters_vector_search(embedding, clusters_coll, similarity_threshold=0.5):
    """
    Busca clusters similares usando pesquisa vetorial.
    
    Args:
        embedding: O vetor de embedding a ser pesquisado
        clusters_coll: Coleção de clusters no MongoDB
        similarity_threshold: Limite mínimo de similaridade (default: 0.5)
        
    Returns:
        tuple: (cluster_document, similarity_score) do cluster mais similar, ou (None, 0) se não encontrado
    """
    try:
        # Realizar pesquisa vetorial
        results = clusters_coll.aggregate([
            {
                "$vectorSearch": {
                    "index": "vector_index_loop_cluster",
                    "path": "embedding",
                    "queryVector": embedding,
                    "numCandidates": 10,
                    "limit": 5,
                }
            },
            {
                "$project": {
                    "similarityScore": {"$meta": "vectorSearchScore"},
                    "document": "$$ROOT",
                }
            },
        ])
        
        results_list = list(results)
        if results_list:
            most_similar = results_list[0]
            similarity_score = most_similar["similarityScore"]
            if similarity_score >= similarity_threshold:
                return most_similar["document"], similarity_score
    
    except Exception as e:
        logger.warning(f"[CLUSTERING] Erro ao realizar pesquisa vetorial: {str(e)}")
    
    return None, 0


def verificar_clusters_existentes(clusters, clusters_coll):
    """
    Verifica quais clusters já existem e quais precisam ser atualizados ou inseridos,
    usando exclusivamente pesquisa vetorial baseada em embeddings.
    
    Regras de similaridade:
    - Similaridade 85-100%: Apenas atualiza posts_ids, mantém summary existente (update_type="merge_only")
    - Similaridade 50-85%: Atualiza posts_ids e marca para reprocessamento do summary (update_type="reprocess")
    - Abaixo de 50%: Considera como novo cluster
    """
    logger.info("[CLUSTERING] Verificando existência de clusters similares antes da inserção")
    
    # Preparar resultado
    clusters_to_insert = []
    clusters_to_update = []
    
    # Definir thresholds de similaridade
    HIGH_SIMILARITY = 0.9
    MEDIUM_SIMILARITY = 0.5
    
    # Função para processar um cluster em paralelo
    def process_cluster(cluster):
        # Se o cluster não tem embedding, marca para inserção
        if "embedding" not in cluster:
            logger.info(f"[CLUSTERING] Cluster sem embedding (label {cluster.get('label', 'N/A')}), marcando como novo")
            return {
                "action": "insert",
                "cluster": cluster
            }
            
        # Usar pesquisa vetorial com threshold médio
        similar_cluster, similarity_score = find_similar_clusters_vector_search(
            cluster["embedding"], 
            clusters_coll, 
            similarity_threshold=MEDIUM_SIMILARITY
        )
        
        if similar_cluster:
            # Encontrou um cluster similar via pesquisa vetorial
            match_percentage = similarity_score * 100
            
            # Calcular união dos posts_ids para atualização
            new_posts_ids = cluster["posts_ids"]
            existing_posts_ids = similar_cluster.get("posts_ids", [])
            new_posts_set = set(new_posts_ids)
            existing_posts_set = set(existing_posts_ids)
            merged_posts_ids = list(new_posts_set.union(existing_posts_set))
            
            # Verificar o nível de similaridade para determinar a ação
            if similarity_score >= HIGH_SIMILARITY:
                # Alta similaridade (90-100%) - apenas atualizar posts_ids, manter summary E embedding
                logger.info(f"[CLUSTERING] Cluster similar encontrado com alta similaridade ({match_percentage:.2f}%) - apenas atualizando posts")
                return {
                    "action": "update",
                    "cluster_id": similar_cluster["_id"],
                    "posts_ids": merged_posts_ids,
                    "was_processed": True,  # Mantém o status processado para não refazer o summary
                    # Não inclui o embedding para preservar o embedding existente
                    "similarity_score": similarity_score,
                    "similarity_level": "high",
                    "update_type": "merge_only",  # Novo flag indicando apenas mesclagem de posts
                    "newest_post_date": cluster.get("newest_post_date", datetime.now())
                }
            else:
                # Média similaridade (50-85%) - atualizar posts_ids, refazer summary e atualizar embedding posteriormente
                logger.info(f"[CLUSTERING] Cluster similar encontrado com média similaridade ({match_percentage:.2f}%) - atualizando posts e marcando para reprocessamento")
                return {
                    "action": "update",
                    "cluster_id": similar_cluster["_id"],
                    "posts_ids": merged_posts_ids,
                    "was_processed": False,  # Marca para reprocessamento
                    "embedding": cluster.get("embedding"),  # Mantém o embedding temporariamente até reprocessamento
                    "similarity_score": similarity_score,
                    "similarity_level": "medium",
                    "update_type": "reprocess",  # Novo flag indicando necessidade de reprocessamento
                    "newest_post_date": cluster.get("newest_post_date", datetime.now())
                }
        else:
            # Nenhuma correspondência significativa por embedding
            logger.info(f"[CLUSTERING] Nenhum cluster semanticamente similar encontrado para label {cluster.get('label', 'N/A')}, criando novo")
            return {
                "action": "insert",
                "cluster": cluster
            }
    
    # Processar clusters em paralelo
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(process_cluster, clusters))
    
    # Processar resultados
    high_similarity_updates = 0
    medium_similarity_updates = 0
    
    for result in results:
        if result["action"] == "update":
            update_info = {
                "cluster_id": result["cluster_id"],
                "posts_ids": result["posts_ids"],
                "was_processed": result["was_processed"],
                "update_type": result.get("update_type", "reprocess"),  # Default para compatibilidade
                "newest_post_date": result.get("newest_post_date")
            }
            
            # Adicionar embedding apenas se presente no resultado E não for alta similaridade
            if "embedding" in result:
                update_info["embedding"] = result["embedding"]
            
            clusters_to_update.append(update_info)
            
            # Contabilizar por nível de similaridade
            if result.get("similarity_level") == "high":
                high_similarity_updates += 1
            elif result.get("similarity_level") == "medium":
                medium_similarity_updates += 1
                
            # Log com percentual de similaridade
            if "similarity_score" in result:
                logger.info(f"[CLUSTERING] Cluster será atualizado com similaridade de {result['similarity_score']*100:.2f}% ({result.get('similarity_level', 'unknown')}) - tipo: {result.get('update_type', 'unknown')}")
        elif result["action"] == "insert":
            clusters_to_insert.append(result["cluster"])
    
    # Estatísticas finais
    logger.info(f"[CLUSTERING] Verificação concluída: {len(clusters_to_insert)} clusters para inserção, {len(clusters_to_update)} para atualização")
    logger.info(f"[CLUSTERING] Detalhamento das atualizações: {high_similarity_updates} alta similaridade (merge_only), {medium_similarity_updates} média similaridade (reprocess)")
    
    return clusters_to_insert, clusters_to_update


def atualizar_clusters_existentes(clusters_to_update, clusters_coll, posts_coll=None):
    """
    Atualiza os clusters existentes no MongoDB.
    Inclui a atualização da data mais recente do post (newest_post_date).
    Otimizada para minimizar consultas ao banco e paralelizar operações.
    Agora usa a flag update_type para distinguir entre tipos de atualizações.
    """
    if not clusters_to_update:
        return 0
    
    logger.info(f"[CLUSTERING] Atualizando {len(clusters_to_update)} clusters existentes")
    
    # Contagem por tipo de atualização para logging
    count_by_type = {"merge_only": 0, "reprocess": 0, "other": 0}
    
    # Preparar conexão com posts se necessário
    if posts_coll is None and db_name_stkfeed:
        posts_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="posts")
    
    # Coletar todos os IDs de posts que precisam de consulta de data
    all_clusters_without_date = []
    all_posts_ids = []
    
    for i, update_info in enumerate(clusters_to_update):
        posts_ids = update_info.get("posts_ids", [])
        newest_date = update_info.get("newest_post_date")
        
        # Contabilizar por tipo de atualização
        update_type = update_info.get("update_type", "other")
        if update_type in count_by_type:
            count_by_type[update_type] += 1
        else:
            count_by_type["other"] += 1
        
        # Se não temos data ou precisamos verificar a data mais recente
        if not newest_date and posts_coll and posts_ids:
            all_clusters_without_date.append(i)
            all_posts_ids.extend([ObjectId(pid) for pid in posts_ids])
    
    # Log das estatísticas de tipos de atualização
    logger.info(f"[CLUSTERING] Distribuição de tipos de atualização: merge_only={count_by_type['merge_only']}, reprocess={count_by_type['reprocess']}, outros={count_by_type['other']}")
    
    # Mapeamento de post_id para data e cluster_index
    post_dates = {}
    posts_by_cluster = {}
    
    # Buscar as datas mais recentes de todos os posts em uma única consulta
    if all_posts_ids and posts_coll:
        logger.info(f"[CLUSTERING] Buscando datas para {len(all_posts_ids)} posts de {len(all_clusters_without_date)} clusters")
        try:
            # Buscar todos os posts com suas datas em uma única consulta
            posts_with_dates = posts_coll.find(
                {"_id": {"$in": all_posts_ids}},
                {"_id": 1, "created_at": 1}
            ).sort("created_at", -1)
            
            # Processar os resultados
            for post in posts_with_dates:
                post_id = str(post["_id"])
                post_dates[post_id] = post.get("created_at")
                
                # Associar cada post ao seu cluster correspondente
                for i in all_clusters_without_date:
                    update_info = clusters_to_update[i]
                    if post_id in update_info.get("posts_ids", []):
                        if i not in posts_by_cluster:
                            posts_by_cluster[i] = []
                        posts_by_cluster[i].append((post_id, post.get("created_at")))
            
            logger.info(f"[CLUSTERING] Encontradas datas para {len(post_dates)} posts")
        except Exception as e:
            logger.error(f"[CLUSTERING] Erro ao buscar datas de posts: {str(e)}")
    
    # Atualizar cada cluster com as informações encontradas
    with ThreadPoolExecutor(max_workers=10) as executor:
        def prepare_update_operation(idx):
            update_info = clusters_to_update[idx]
            posts_ids = update_info.get("posts_ids", []).copy()  # Fazer uma cópia para não alterar o original
            newest_date = update_info.get("newest_post_date")
            update_type = update_info.get("update_type", "reprocess")  # Default para compatibilidade
            
            # Verificar se temos informações de data para este cluster
            if idx in posts_by_cluster and posts_by_cluster[idx]:
                # Ordenar os posts do cluster por data (mais recente primeiro)
                cluster_posts = sorted(posts_by_cluster[idx], key=lambda x: x[1], reverse=True)
                
                # Pegar o post mais recente
                most_recent_id, most_recent_date = cluster_posts[0]
                newest_date = most_recent_date
                
                # Reorganizar a lista para ter o post mais recente primeiro
                if most_recent_id in posts_ids:
                    posts_ids.remove(most_recent_id)
                    posts_ids.insert(0, most_recent_id)
            
            # Preparar operação de atualização
            update_data = {
                "posts_ids": posts_ids,  # Lista com o post mais recente primeiro
                "was_processed": update_info["was_processed"],
                "was_updated": True,
                "update_type": update_type  # Novo campo indicando o tipo de atualização
            }
            
            # Adicionar embedding APENAS quando for um reprocessamento (média similaridade)
            # Isso preserva o embedding existente para clusters de alta similaridade
            if update_type == "reprocess" and "embedding" in update_info:
                update_data["embedding"] = update_info["embedding"]
                logger.info(f"[CLUSTERING] Atualizando embedding para cluster {update_info['cluster_id']} (tipo: reprocess)")
            
            # Adicionar newest_post_date se disponível
            if newest_date:
                update_data["newest_post_date"] = newest_date
            
            return pymongo.UpdateOne(
                {"_id": update_info["cluster_id"]},
                {"$set": update_data}
            )
        
        # Executar preparação das operações em paralelo
        bulk_operations = list(executor.map(
            prepare_update_operation, 
            range(len(clusters_to_update))
        ))
    
    # Executar todas as atualizações de uma vez
    if bulk_operations:
        start_time = time.time()
        result = clusters_coll.bulk_write(bulk_operations)
        elapsed_time = time.time() - start_time
        logger.info(f"[CLUSTERING] {result.modified_count} clusters atualizados com sucesso em {elapsed_time:.2f} segundos")
        return result.modified_count
    
    return 0


def inserir_novos_clusters(clusters_to_insert, clusters_coll):
    """Insere novos clusters no MongoDB."""
    if clusters_to_insert:
        # Adicionar flag was_updated em cada novo cluster
        for cluster in clusters_to_insert:
            cluster["was_updated"] = False
        
        # Remover post_titles dos clusters antes da inserção
        for cluster in clusters_to_insert:
            cluster.pop("post_titles", None)
            
        # Contar clusters com embedding para logging
        clusters_with_embedding = sum(1 for cluster in clusters_to_insert if "embedding" in cluster)
        if clusters_with_embedding > 0:
            logger.info(f"[CLUSTERING] {clusters_with_embedding} novos clusters contêm centroides (embeddings)")
                
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
        result = obter_posts_com_embeddings(posts_coll, dias=7)
        if not result:
            return
        
        unique_documents, original_documents = result
        
        # Executar o clustering
        labels, post_ids, cluster_counts, centroids = executar_clustering(unique_documents)
        
        # Organizar os resultados por cluster
        clusters, clusters_by_label, clusters_titles_by_label = organizar_clusters_por_label(labels, post_ids, unique_documents, centroids)
        
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
        
        # Logs sobre os centroides
        clusters_with_embedding = sum(1 for cluster in clusters if "embedding" in cluster)
        logger.info(f"[CLUSTERING] Centroides capturados: {len(centroids)} centroides para {clusters_with_embedding} clusters")
        
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
    
    Seleciona clusters que precisam de processamento baseado em:
    1. Clusters nunca processados (was_processed=False)
    2. Clusters marcados para reprocessamento (update_type=reprocess)
    
    Após o processamento, atualiza os embeddings dos clusters baseados nos novos summaries.
    
    Parâmetros:
    - max_workers: Número máximo de workers para processamento paralelo (padrão: 5)
    - model_name: Nome do modelo LLM a ser usado (padrão: "gemini-2.0-flash")
    - max_tokens: Número máximo de tokens na resposta (padrão: 100000)
    - timeout: Tempo máximo de espera em segundos (padrão: 200.0)
    - temperature: Temperatura para geração de respostas (padrão: 1.0)
    """
    logger.info(f"[PROCESSO-CLUSTERS] Iniciando processamento de clusters em paralelo (max_workers={max_workers}, model={model_name})")
    
    # Registrar o tempo de início
    start_time = time.time()
    
    try:
        clusters_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="clusters")
        posts_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="posts")
        
        logger.info("[PROCESSO-CLUSTERS] Conectado às coleções no MongoDB")
        
        # Encontrar clusters que precisam ser processados: não processados ou marcados para reprocessamento
        logger.info("[PROCESSO-CLUSTERS] Buscando clusters que precisam de processamento")
        
        # Query usando $or para buscar tanto was_processed=False quanto update_type=reprocess
        unprocessed_clusters = list(clusters_coll.find(
            {
                "$or": [
                    {"was_processed": False},  # Clusters nunca processados
                    {"update_type": "reprocess"}  # Clusters marcados para reprocessamento
                ],
                "label": {"$ne": -1}  # Excluir ruído
            },
            {"_id": 1, "posts_ids": 1, "label": 1, "update_type": 1}
        ))
        
        if not unprocessed_clusters:
            logger.info("[PROCESSO-CLUSTERS] Não há clusters para processar")
            return
        
        total_clusters = len(unprocessed_clusters)
        
        # Contar clusters por tipo para logging
        new_clusters = sum(1 for c in unprocessed_clusters if not c.get("update_type"))
        reprocess_clusters = sum(1 for c in unprocessed_clusters if c.get("update_type") == "reprocess")
        
        logger.info(f"[PROCESSO-CLUSTERS] Encontrados {total_clusters} clusters para processar:")
        logger.info(f"[PROCESSO-CLUSTERS] - {new_clusters} clusters novos (nunca processados)")
        logger.info(f"[PROCESSO-CLUSTERS] - {reprocess_clusters} clusters para reprocessamento (update_type=reprocess)")
        
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
                "post_dates": [],
                "update_type": cluster.get("update_type", "new")  # Armazenar tipo de atualização
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
                
            logger.info(f"[PROCESSO-CLUSTERS] Preparando prompt para cluster {cluster_id} com {len(posts)} posts (tipo: {cluster_info.get('update_type', 'new')})")
            
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
                update_type = cluster_info.get("update_type", "new")
                
                logger.info(f"[PROCESSO-CLUSTERS] Processando resposta {i+1}/{len(raw_responses)} para cluster {cluster_id} (tipo: {update_type})")
                
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
                    "users_ids": list(cluster_info["users_ids"]),
                    "update_type": "none"  # Resetar update_type após processamento
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
                
                # Após atualizar os clusters com summaries, gerar embeddings para eles
                if result.modified_count > 0:
                    logger.info("[PROCESSO-CLUSTERS] Iniciando geração de embeddings para clusters processados")
                    embedding_result = gerar_embeddings_clusters()
                    logger.info(f"[PROCESSO-CLUSTERS] Embeddings gerados para {embedding_result.get('processed', 0)} clusters")
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
            



# prompt1 = "OpenAI is reportedly considering acquiring io Products, an AI hardware startup founded by former Apple design chief Jony Ive and backed by OpenAI CEO Sam Altman, potentially valuing the company at over $500M. The startup is developing AI-powered personal devices, including concepts like 'phones without screens', aiming to create a less disruptive AI experience. A successful product could represent a pivotal moment for AI hardware, akin to the impact of the iPhone."
# prompt2 = "OpenAI is considering acquiring io Products, an AI hardware startup led by former Apple design chief Jony Ive and backed by OpenAI CEO Sam Altman, with potential valuations exceeding $500M. The startup aims to develop AI-enabled devices, possibly including 'phones without screens,' with the goal of creating a less socially disruptive and more innovative product than current smartphones. Discussions involve exploring partnership options as well as a full acquisition, signaling OpenAI's strategic interest in shaping the future of AI hardware and potentially creating a disruptive 'iPhone moment' for the industry."

# embeddings = get_embedding(prompt1)
# embeddings2 = get_embedding(prompt2)

# import numpy as np
# #function to calculate cosine similarity from embeddings
# def calculate_cosine_similarity(embeddings, embeddings2):
#     return np.dot(embeddings, embeddings2) / (np.linalg.norm(embeddings) * np.linalg.norm(embeddings2))

# cosine_similarity = calculate_cosine_similarity(embeddings, embeddings2)

# print(cosine_similarity)

def gerar_embeddings_clusters(max_workers=10, batch_size=200):
    """
    Gera embeddings para o campo summary de todos os clusters existentes que ainda não têm embeddings.
    
    Utiliza paralelização para melhorar a performance.
    
    Args:
        max_workers (int): Número máximo de workers para paralelização
        batch_size (int): Tamanho do lote de clusters para processar de cada vez
    
    Returns:
        dict: Estatísticas do processamento
    """
    logger.info(f"[CLUSTERS-EMBEDDINGS] Iniciando geração de embeddings para clusters (max_workers={max_workers})")
    start_time = time.time()
    
    try:
        # Conectar ao MongoDB
        clusters_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="clusters")
        
        # Encontrar clusters que têm summary mas não têm embedding
        query = {
            "summary": {"$exists": True, "$ne": ""},
            "$or": [
                {"embedding": {"$exists": False}},
                {"embedding": None}
            ]
        }
        
        # Contar total de clusters para processar
        total_clusters = clusters_coll.count_documents(query)
        logger.info(f"[CLUSTERS-EMBEDDINGS] Encontrados {total_clusters} clusters para gerar embeddings")
        
        if total_clusters == 0:
            logger.info("[CLUSTERS-EMBEDDINGS] Nenhum cluster para processar")
            return {"processed": 0, "errors": 0, "elapsed_time": 0}
        
        # Processar em lotes para não sobrecarregar a memória
        processed_count = 0
        error_count = 0
        
        # Função para processar um único cluster em paralelo
        def process_cluster(cluster):
            try:
                cluster_id = cluster["_id"]
                summary = cluster.get("summary", "")
                
                if not summary:
                    logger.warning(f"[CLUSTERS-EMBEDDINGS] Cluster {cluster_id} não tem summary válido")
                    return None
                
                # Gerar embedding para o summary
                embedding = get_embedding(summary)
                if not embedding:
                    logger.error(f"[CLUSTERS-EMBEDDINGS] Falha ao gerar embedding para cluster {cluster_id}")
                    return None
                
                return {
                    "cluster_id": cluster_id,
                    "embedding": embedding
                }
            except Exception as e:
                logger.error(f"[CLUSTERS-EMBEDDINGS] Erro ao processar cluster {cluster.get('_id')}: {str(e)}")
                return None
        
        # Processar em lotes
        offset = 0
        while offset < total_clusters:
            batch = list(clusters_coll.find(query).skip(offset).limit(batch_size))
            if not batch:
                break
            
            logger.info(f"[CLUSTERS-EMBEDDINGS] Processando lote de {len(batch)} clusters (offset: {offset})")
            
            # Processar lote em paralelo
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                results = list(executor.map(process_cluster, batch))
            
            # Filtrar resultados válidos
            valid_results = [r for r in results if r is not None]
            
            # Atualizar clusters no MongoDB usando bulk_write
            if valid_results:
                bulk_operations = [
                    pymongo.UpdateOne(
                        {"_id": result["cluster_id"]},
                        {"$set": {"embedding": result["embedding"]}}
                    ) for result in valid_results
                ]
                
                if bulk_operations:
                    result = clusters_coll.bulk_write(bulk_operations)
                    logger.info(f"[CLUSTERS-EMBEDDINGS] Atualizados {result.modified_count} clusters neste lote")
                    processed_count += result.modified_count
            
            # Atualizar contagem de erros
            error_count += len(batch) - len(valid_results)
            
            # Avançar para o próximo lote
            offset += batch_size
        
        # Calcular tempo de processamento
        end_time = time.time()
        elapsed_time = end_time - start_time
        minutes = int(elapsed_time // 60)
        seconds = elapsed_time % 60
        
        # Log final
        logger.info(f"[CLUSTERS-EMBEDDINGS] Processamento concluído em {minutes} minutos e {seconds:.2f} segundos")
        logger.info(f"[CLUSTERS-EMBEDDINGS] Total de clusters: {total_clusters}")
        logger.info(f"[CLUSTERS-EMBEDDINGS] Processados com sucesso: {processed_count}")
        logger.info(f"[CLUSTERS-EMBEDDINGS] Erros: {error_count}")
        
        return {
            "processed": processed_count,
            "errors": error_count,
            "elapsed_time": elapsed_time
        }
    
    except Exception as e:
        logger.error(f"[CLUSTERS-EMBEDDINGS] ERRO CRÍTICO durante geração de embeddings: {str(e)}")
        logger.error(f"[CLUSTERS-EMBEDDINGS] Traceback completo: {traceback.format_exc()}")
        
        # Calcular tempo mesmo em caso de erro
        end_time = time.time()
        elapsed_time = end_time - start_time
        minutes = int(elapsed_time // 60)
        seconds = elapsed_time % 60
        logger.error(f"[CLUSTERS-EMBEDDINGS] Processo falhou após {minutes} minutos e {seconds:.2f} segundos")
        
        return {
            "processed": 0,
            "errors": 1,
            "elapsed_time": elapsed_time,
            "error": str(e)
        }
            

# clustering_posts()
# process_clusters()
# generate_trends_from_clusters()
# reorganizar_trends_posts()
# reorganizar_clusters_posts()

def reorganizar_clusters_posts(max_workers=20, batch_size=100):
    """
    Percorre todos os clusters da coleção, reordena os posts_ids com o mais recente primeiro,
    e atualiza o campo newest_post_date para a data do post mais recente.
    
    Todas as operações são paralelizadas para maximizar a eficiência.
    
    Args:
        max_workers (int): Número máximo de workers para paralelização (default: 20)
        batch_size (int): Tamanho do lote de clusters para processar por vez (default: 100)
        
    Returns:
        dict: Estatísticas de processamento (total de clusters, sucessos, erros, tempo)
    """
    logger.info(f"[CLUSTERS-REORGANIZAR] Iniciando reorganização de posts nos clusters (max_workers={max_workers})")
    start_time = time.time()
    
    try:
        # Conectar às coleções
        clusters_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="clusters")
        posts_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="posts")
        
        # Contar total de clusters para processar
        total_clusters = clusters_coll.count_documents({})
        logger.info(f"[CLUSTERS-REORGANIZAR] Encontrados {total_clusters} clusters para processar")
        
        if total_clusters == 0:
            logger.info("[CLUSTERS-REORGANIZAR] Nenhum cluster para processar")
            return {"total": 0, "success": 0, "errors": 0, "elapsed_time": 0}
        
        # Contadores para estatísticas
        processed_count = 0
        error_count = 0
        update_count = 0
        
        # Processar clusters em lotes para gerenciar memória
        offset = 0
        
        while offset < total_clusters:
            # Buscar lote de clusters
            logger.info(f"[CLUSTERS-REORGANIZAR] Processando lote de clusters ({offset} a {offset + batch_size})")
            batch = list(clusters_coll.find({}).skip(offset).limit(batch_size))
            
            if not batch:
                break
                
            # Coletar todos os post_ids de todos os clusters no lote para uma única consulta
            all_post_ids = []
            post_id_to_cluster_map = {}  # Mapear post_id -> lista de cluster_ids
            
            for cluster in batch:
                cluster_id = cluster["_id"]
                post_ids = cluster.get("posts_ids", [])
                
                if not post_ids:
                    logger.warning(f"[CLUSTERS-REORGANIZAR] Cluster {cluster_id} não tem posts")
                    continue
                
                # Converter para ObjectId para consulta
                for post_id in post_ids:
                    try:
                        obj_id = ObjectId(post_id)
                        all_post_ids.append(obj_id)
                        
                        # Mapear este post_id para este cluster
                        if post_id not in post_id_to_cluster_map:
                            post_id_to_cluster_map[post_id] = []
                        post_id_to_cluster_map[post_id].append(cluster_id)
                    except Exception as e:
                        logger.warning(f"[CLUSTERS-REORGANIZAR] ID de post inválido: {post_id}, erro: {e}")
            
            # Remover duplicatas - um post pode estar em múltiplos clusters
            unique_post_ids = list(set(all_post_ids))
            if not unique_post_ids:
                logger.warning(f"[CLUSTERS-REORGANIZAR] Nenhum ID de post válido encontrado no lote atual")
                offset += batch_size
                continue
            
            # Buscar todos os posts com datas em uma única consulta
            logger.info(f"[CLUSTERS-REORGANIZAR] Buscando {len(unique_post_ids)} posts únicos")
            posts_with_dates = list(posts_coll.find(
                {"_id": {"$in": unique_post_ids}},
                {"_id": 1, "created_at": 1}
            ).sort("created_at", -1))
            
            # Criar dicionário post_id -> created_at
            post_dates = {}
            for post in posts_with_dates:
                post_id = str(post["_id"])
                created_at = post.get("created_at")
                if created_at:
                    post_dates[post_id] = created_at
            
            logger.info(f"[CLUSTERS-REORGANIZAR] Obtidas datas para {len(post_dates)} posts")
            
            # Organizar posts por cluster e ordenar
            clusters_data = {}
            for cluster in batch:
                cluster_id = cluster["_id"]
                post_ids = cluster.get("posts_ids", [])
                
                # Filtrar apenas posts que temos data
                valid_posts = [(pid, post_dates.get(pid)) for pid in post_ids if pid in post_dates]
                
                if not valid_posts:
                    logger.warning(f"[CLUSTERS-REORGANIZAR] Cluster {cluster_id} não tem posts com datas válidas")
                    continue
                
                # Ordenar posts por data (mais recente primeiro)
                sorted_posts = sorted(valid_posts, key=lambda x: x[1] if x[1] else datetime.min, reverse=True)
                
                # Pegar IDs ordenados e a data mais recente
                ordered_post_ids = [p[0] for p in sorted_posts]
                newest_date = sorted_posts[0][1] if sorted_posts else None
                
                # Armazenar para atualização
                clusters_data[cluster_id] = {
                    "ordered_post_ids": ordered_post_ids,
                    "newest_date": newest_date
                }
            
            # Função para processar cada cluster em paralelo
            def process_cluster(cluster_id):
                try:
                    if cluster_id not in clusters_data:
                        return {"success": False, "cluster_id": cluster_id, "reason": "No data"}
                    
                    data = clusters_data[cluster_id]
                    ordered_post_ids = data["ordered_post_ids"]
                    newest_date = data["newest_date"]
                    
                    # Criar operação de atualização
                    update_fields = {
                        "posts_ids": ordered_post_ids
                    }
                    
                    # Atualizar newest_post_date apenas se temos data do post mais recente
                    if newest_date:
                        update_fields["newest_post_date"] = newest_date
                    
                    # Executar atualização
                    result = clusters_coll.update_one(
                        {"_id": cluster_id},
                        {"$set": update_fields}
                    )
                    
                    return {
                        "success": result.modified_count > 0,
                        "cluster_id": cluster_id,
                        "modified": result.modified_count
                    }
                except Exception as e:
                    logger.error(f"[CLUSTERS-REORGANIZAR] Erro ao processar cluster {cluster_id}: {str(e)}")
                    return {"success": False, "cluster_id": cluster_id, "error": str(e)}
            
            # Processar clusters em paralelo
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                cluster_ids = list(clusters_data.keys())
                results = list(executor.map(process_cluster, cluster_ids))
            
            # Processar resultados
            batch_success = sum(1 for r in results if r.get("success", False))
            batch_errors = len(results) - batch_success
            
            processed_count += len(results)
            update_count += batch_success
            error_count += batch_errors
            
            logger.info(f"[CLUSTERS-REORGANIZAR] Lote processado: {batch_success} clusters atualizados, {batch_errors} erros")
            
            # Avançar para o próximo lote
            offset += batch_size
        
        # Calcular estatísticas finais
        end_time = time.time()
        elapsed_time = end_time - start_time
        minutes = int(elapsed_time // 60)
        seconds = elapsed_time % 60
        
        logger.info(f"[CLUSTERS-REORGANIZAR] Processamento concluído em {minutes}m {seconds:.2f}s")
        logger.info(f"[CLUSTERS-REORGANIZAR] Total processado: {processed_count} clusters")
        logger.info(f"[CLUSTERS-REORGANIZAR] Atualizados com sucesso: {update_count} clusters")
        logger.info(f"[CLUSTERS-REORGANIZAR] Erros: {error_count} clusters")
        
        return {
            "total": total_clusters,
            "processed": processed_count,
            "success": update_count,
            "errors": error_count,
            "elapsed_time": elapsed_time
        }
    
    except Exception as e:
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.error(f"[CLUSTERS-REORGANIZAR] ERRO CRÍTICO: {str(e)}")
        logger.error(f"[CLUSTERS-REORGANIZAR] Traceback: {traceback.format_exc()}")
        
        return {
            "success": False,
            "error": str(e),
            "elapsed_time": elapsed_time
        }

