from util.mongodb_utils import get_mongo_collection
from env import db_name_stkfeed
import logging
from datetime import datetime, timedelta
from bson.objectid import ObjectId
import re
import traceback
import time
import pymongo
import json

logger = logging.getLogger(__name__)



def format_time_ago(date):
    """
    Formata a data para o formato "X time ago" (por exemplo, "2 hours ago", "3 days ago").
    
    Args:
        date (datetime): Data a ser formatada
        
    Returns:
        str: String formatada no estilo "X time ago"
    """
    now = datetime.utcnow()
    diff = now - date
    
    if diff.days > 30:
        months = diff.days // 30
        return f"{months} {'month' if months == 1 else 'months'} ago"
    elif diff.days > 0:
        return f"{diff.days} {'day' if diff.days == 1 else 'days'} ago"
    elif diff.seconds >= 3600:
        hours = diff.seconds // 3600
        return f"{hours} {'hour' if hours == 1 else 'hours'} ago"
    elif diff.seconds >= 60:
        minutes = diff.seconds // 60
        return f"{minutes} {'minute' if minutes == 1 else 'minutes'} ago"
    else:
        return "just now"

def generate_trends_from_clusters():
    """
    Gera e atualiza trends a partir dos clusters processados:
    1. Atualiza trends existentes para clusters marcados com was_updated=True
    2. Cria novas trends para clusters processados que ainda não têm trends associadas
    
    Otimizações implementadas:
    - Utiliza bulk_write para atualizar todas as trends de uma só vez
    - Usa pipeline de agregação eficiente para encontrar clusters sem trends
    - Processamento otimizado para grandes volumes de dados
    """
    logger.info("[TRENDS] Iniciando geração e atualização de trends a partir de clusters...")
    
    start_time = time.time()
    
    try:
        # Conectar às coleções
        logger.info("[TRENDS] Conectando às coleções no MongoDB")
        clusters_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="clusters")
        trends_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="trends")
        
        # Disclaimer padrão
        default_disclaimer = "This story is a summary of posts and may evolve over time."
        
        # 1. ATUALIZAR TRENDS EXISTENTES PARA CLUSTERS ATUALIZADOS
        logger.info("[TRENDS] Buscando clusters atualizados com trends existentes")
        
        # Buscar todos os clusters que foram atualizados (was_updated=True)
        updated_clusters = list(clusters_coll.find(
            {
                "was_updated": True, 
                "was_processed": True, 
                "relevance_score": {"$gte": 0.2}
            },
            {
                "_id": 1, "theme": 1, "summary": 1, "posts_ids": 1, "key_points": 1, 
                "relevance_score": 1, "dispersion_score": 1, "newest_post_date": 1,
                "stakeholder_impact": 1, "sector_specific": 1,
                "users_ids": 1
            }
        ))
        #limit the len(users_ids) to 100
        updated_clusters = [cluster for cluster in updated_clusters if len(cluster.get("users_ids", [])) <= 100]
        
        logger.info(f"[TRENDS] Encontrados {len(updated_clusters)} clusters atualizados para verificar")
        
        # Buscar todas as trends existentes para os clusters atualizados em uma só consulta
        if updated_clusters:
            cluster_ids = [str(cluster["_id"]) for cluster in updated_clusters]
            existing_trends = list(trends_coll.find({"cluster_id": {"$in": cluster_ids}}))
            
            # Criar mapa de cluster_id para trend para acesso rápido
            trend_by_cluster_id = {trend["cluster_id"]: trend for trend in existing_trends}
            logger.info(f"[TRENDS] Encontradas {len(existing_trends)} trends existentes para atualizar")
        else:
            trend_by_cluster_id = {}
        
        # Preparar operações em lote para atualização
        update_operations = []
        updated_cluster_count = 0
        
        for cluster in updated_clusters:
            cluster_id = str(cluster["_id"])
            
            # Verificar se existe trend para este cluster
            if cluster_id in trend_by_cluster_id:
                existing_trend = trend_by_cluster_id[cluster_id]
                logger.info(f"[TRENDS] Preparando atualização para trend do cluster: {cluster_id}")
                
                # Formatar data de última atualização
                last_updated = "recently"
                if "newest_post_date" in cluster and cluster["newest_post_date"]:
                    newest_date = cluster["newest_post_date"]
                    if isinstance(newest_date, str):
                        try:
                            newest_date = datetime.fromisoformat(newest_date.replace('Z', '+00:00'))
                        except ValueError:
                            try:
                                newest_date = datetime.strptime(newest_date, "%Y-%m-%dT%H:%M:%S.%fZ")
                            except ValueError:
                                logger.warning(f"[TRENDS] Formato de data não reconhecido: {newest_date}, usando data atual")
                                newest_date = datetime.now()
                    last_updated = format_time_ago(newest_date)
                
                # Construir summary completo com key points, riscos e oportunidades
                summary = cluster.get("summary", "")
                if not summary:
                    logger.warning(f"[TRENDS] Cluster {cluster_id} não possui resumo")
                
                # Incluir pontos-chave 
                if cluster.get("key_points"):
                    summary += "\n\nKey Points:\n"
                    for point in cluster.get("key_points"):
                        summary += f"- {point}\n"
                
                # Incluir riscos e oportunidades
                if cluster.get("sector_specific"):
                    risks = cluster.get("sector_specific", {}).get("risks", [])
                    opportunities = cluster.get("sector_specific", {}).get("opportunities", [])
                    
                    if risks:
                        summary += "\nRisks:\n"
                        for risk in risks:
                            summary += f"- {risk}\n"
                    
                    if opportunities:
                        summary += "\nOpportunities:\n"
                        for opportunity in opportunities:
                            summary += f"- {opportunity}\n"
                
                # Adicionar operação de atualização ao lote
                update_operations.append(
                    pymongo.UpdateOne(
                        {"_id": existing_trend["_id"]},
                        {"$set": {
                            "title": cluster.get("theme", "Untitled Trend"),
                            "posts": len(cluster.get("posts_ids", [])),
                            "summary": summary,
                            "lastUpdated": last_updated,
                            "updated_at": cluster.get("newest_post_date", datetime.utcnow()),
                            "postIds": cluster.get("posts_ids", []),
                            "key_points": cluster.get("key_points", []),
                            "relevance_score": cluster.get("relevance_score", 0),
                            "dispersion_score": cluster.get("dispersion_score", 0),
                            "stakeholder_impact": cluster.get("stakeholder_impact", ""),
                            "sector_specific": cluster.get("sector_specific", {"opportunities": [], "risks": []})
                        }}
                    )
                )
                updated_cluster_count += 1
                logger.info(f"[TRENDS] Trend preparada para atualização: '{cluster.get('theme', 'Untitled Trend')}' com {len(cluster.get('posts_ids', []))} posts")
        
        # Executar todas as atualizações em lote
        if update_operations:
            start_update_time = time.time()
            logger.info(f"[TRENDS] Executando atualização em lote para {len(update_operations)} trends")
            update_result = trends_coll.bulk_write(update_operations)
            update_time = time.time() - start_update_time
            
            logger.info(f"[TRENDS] Atualização em lote concluída em {update_time:.2f} segundos")
            logger.info(f"[TRENDS] Trends atualizadas: {update_result.modified_count}")
        else:
            logger.info("[TRENDS] Nenhuma trend para atualizar")
        
        # 2. CRIAR NOVAS TRENDS PARA CLUSTERS SEM TRENDS
        logger.info("[TRENDS] Buscando clusters processados sem trends associadas")
        
        # Usar aggregation para encontrar clusters que não têm trends associadas
        start_query_time = time.time()
        pipeline = [
            # Match para encontrar clusters processados com relevância adequada
            {"$match": {
                "was_processed": True,
                "relevance_score": {"$gte": 0.2}
            }},
            # Lookup para verificar se existem trends para estes clusters
            {"$lookup": {
                "from": "trends",
                "localField": "_id",
                "foreignField": "cluster_id",
                "as": "existing_trends"
            }},
            # Filtrar apenas clusters sem trends
            {"$match": {"existing_trends": {"$size": 0}}},
            # Projetar apenas os campos necessários
            {"$project": {
                "_id": 1, "theme": 1, "summary": 1, "posts_ids": 1, "key_points": 1, 
                "relevance_score": 1, "dispersion_score": 1, "newest_post_date": 1,
                "stakeholder_impact": 1, "sector_specific": 1
            }}
        ]
        
        new_clusters = list(clusters_coll.aggregate(pipeline))
        query_time = time.time() - start_query_time
        logger.info(f"[TRENDS] Encontrados {len(new_clusters)} clusters sem trends associadas em {query_time:.2f} segundos")
        
        # Preparar novas trends para inserção em lote
        new_trends = []
        
        for cluster in new_clusters:
            try:
                # Determinar a categoria
                category = "Technology"
                
                # Formatar data de última atualização
                last_updated = "recently"
                if "newest_post_date" in cluster and cluster["newest_post_date"]:
                    newest_date = cluster["newest_post_date"]
                    if isinstance(newest_date, str):
                        try:
                            newest_date = datetime.fromisoformat(newest_date.replace('Z', '+00:00'))
                        except ValueError:
                            try:
                                newest_date = datetime.strptime(newest_date, "%Y-%m-%dT%H:%M:%S.%fZ")
                            except ValueError:
                                logger.warning(f"[TRENDS] Formato de data não reconhecido: {newest_date}, usando data atual")
                                newest_date = datetime.now()
                    last_updated = format_time_ago(newest_date)
                
                # Construir summary completo
                summary = cluster.get("summary", "")
                if not summary:
                    logger.warning(f"[TRENDS] Cluster {cluster['_id']} não possui resumo")
                
                # Incluir pontos-chave 
                if cluster.get("key_points"):
                    summary += "\n\nKey Points:\n"
                    for point in cluster.get("key_points"):
                        summary += f"- {point}\n"
                
                # Incluir riscos e oportunidades
                if cluster.get("sector_specific"):
                    risks = cluster.get("sector_specific", {}).get("risks", [])
                    opportunities = cluster.get("sector_specific", {}).get("opportunities", [])
                    
                    if risks:
                        summary += "\nRisks:\n"
                        for risk in risks:
                            summary += f"- {risk}\n"
                    
                    if opportunities:
                        summary += "\nOpportunities:\n"
                        for opportunity in opportunities:
                            summary += f"- {opportunity}\n"
                
                # Criar trend
                trend = {
                    "title": cluster.get("theme", "Untitled Trend"),
                    "category": category,
                    "posts": len(cluster.get("posts_ids", [])),
                    "summary": summary,
                    "lastUpdated": last_updated,
                    "updated_at": cluster.get("newest_post_date", datetime.utcnow()),
                    "disclaimer": default_disclaimer,
                    "postIds": cluster.get("posts_ids", []),
                    "key_points": cluster.get("key_points", []),
                    "relevance_score": cluster.get("relevance_score", 0),
                    "dispersion_score": cluster.get("dispersion_score", 0),
                    "stakeholder_impact": cluster.get("stakeholder_impact", ""),
                    "sector_specific": cluster.get("sector_specific", {"opportunities": [], "risks": []}),
                    "cluster_id": str(cluster["_id"]),
                    "created_at": datetime.utcnow()
                }
                
                new_trends.append(trend)
                logger.info(f"[TRENDS] Nova trend preparada: '{trend['title']}' com {trend['posts']} posts")
                
            except Exception as e:
                logger.error(f"[TRENDS] ERRO ao processar cluster {cluster['_id']}: {str(e)}")
                logger.error(f"[TRENDS] Traceback: {traceback.format_exc()}")
                # Continua para o próximo cluster mesmo se houver erro
        
        # Inserir novas trends no banco de dados em lote
        if new_trends:
            start_insert_time = time.time()
            logger.info(f"[TRENDS] Inserindo {len(new_trends)} novas trends na coleção")
            insert_result = trends_coll.insert_many(new_trends)
            insert_time = time.time() - start_insert_time
            
            logger.info(f"[TRENDS] {len(insert_result.inserted_ids)} novas trends inseridas com sucesso em {insert_time:.2f} segundos")
        else:
            logger.warning("[TRENDS] Nenhuma nova trend foi criada para inserção")
        
        # 3. RESETAR FLAG WAS_UPDATED NOS CLUSTERS PROCESSADOS EM LOTE
        logger.info("[TRENDS] Resetando flag was_updated em clusters processados")
        reset_start_time = time.time()
        update_result = clusters_coll.update_many(
            {"was_updated": True},
            {"$set": {"was_updated": False}}
        )
        reset_time = time.time() - reset_start_time
        logger.info(f"[TRENDS] Flag was_updated resetada em {update_result.modified_count} clusters em {reset_time:.2f} segundos")
        
        # RESULTADOS
        total_trends = updated_cluster_count + len(new_trends)
        total_time = time.time() - start_time
        minutes = int(total_time // 60)
        seconds = total_time % 60
        
        logger.info(f"[TRENDS] Geração e atualização de trends concluída em {minutes} minutos e {seconds:.2f} segundos")
        logger.info(f"[TRENDS] Total: {total_trends} trends ({updated_cluster_count} atualizadas, {len(new_trends)} novas)")
        
        return {
            "total": total_trends,
            "updated": updated_cluster_count,
            "new": len(new_trends),
            "elapsed_time": total_time
        }
    
    except Exception as e:
        logger.error(f"[TRENDS] ERRO CRÍTICO durante geração de trends: {str(e)}")
        logger.error(f"[TRENDS] Traceback completo: {traceback.format_exc()}")
        
        # Calcular tempo mesmo em caso de erro
        total_time = time.time() - start_time
        minutes = int(total_time // 60)
        seconds = total_time % 60
        logger.error(f"[TRENDS] Processo falhou após {minutes} minutos e {seconds:.2f} segundos")
        
        # Re-lançar exceção para que seja tratada na função update_trends
        raise

# Função principal para ser chamada externamente
def update_trends():
    """
    Atualiza a coleção de trends a partir dos clusters processados.
    """
    logger.info("[TRENDS-UPDATE] Iniciando atualização de trends")
    start_time = datetime.now()
    
    try:
        logger.info("[TRENDS-UPDATE] Chamando função generate_trends_from_clusters")
        trends_info = generate_trends_from_clusters()
        
        execution_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"[TRENDS-UPDATE] Trends atualizadas com sucesso. {trends_info['total']} trends geradas em {execution_time:.2f} segundos")
        
        return {
            "success": True, 
            "message": f"Trends atualizadas com sucesso. {trends_info['total']} trends geradas.",
            "trends_count": trends_info['total'],
            "execution_time_seconds": execution_time
        }
    except Exception as e:
        execution_time = (datetime.now() - start_time).total_seconds()
        logger.error(f"[TRENDS-UPDATE] ERRO ao atualizar trends após {execution_time:.2f} segundos: {str(e)}")
        logger.error(f"[TRENDS-UPDATE] Traceback completo: {traceback.format_exc()}")
        
        return {
            "success": False, 
            "message": f"Erro ao atualizar trends: {str(e)}",
            "error_type": type(e).__name__,
            "execution_time_seconds": execution_time
        } 
    


def reorganizar_trends_posts(max_workers=20, batch_size=100):
    """
    Percorre todas as trends da coleção, reordena os posts com o mais recente primeiro,
    e atualiza o campo updated_at para a data do post mais recente.
    
    Todas as operações são paralelizadas para maximizar a eficiência.
    
    Args:
        max_workers (int): Número máximo de workers para paralelização (default: 20)
        batch_size (int): Tamanho do lote de trends para processar por vez (default: 100)
        
    Returns:
        dict: Estatísticas de processamento (total de trends, sucessos, erros, tempo)
    """
    logger.info(f"[TRENDS-REORGANIZAR] Iniciando reorganização de posts nas trends (max_workers={max_workers})")
    start_time = time.time()
    
    try:
        # Conectar às coleções
        trends_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="trends")
        posts_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="posts")
        
        # Contar total de trends para processar
        total_trends = trends_coll.count_documents({})
        logger.info(f"[TRENDS-REORGANIZAR] Encontradas {total_trends} trends para processar")
        
        if total_trends == 0:
            logger.info("[TRENDS-REORGANIZAR] Nenhuma trend para processar")
            return {"total": 0, "success": 0, "errors": 0, "elapsed_time": 0}
        
        # Contadores para estatísticas
        processed_count = 0
        error_count = 0
        update_count = 0
        
        # Processar trends em lotes para gerenciar memória
        offset = 0
        
        while offset < total_trends:
            # Buscar lote de trends
            logger.info(f"[TRENDS-REORGANIZAR] Processando lote de trends ({offset} a {offset + batch_size})")
            batch = list(trends_coll.find({}).skip(offset).limit(batch_size))
            
            if not batch:
                break
                
            # Coletar todos os post_ids de todas as trends no lote para uma única consulta
            all_post_ids = []
            post_id_to_trend_map = {}  # Mapear post_id -> lista de trend_ids
            
            for trend in batch:
                trend_id = trend["_id"]
                post_ids = trend.get("postIds", [])
                
                if not post_ids:
                    logger.warning(f"[TRENDS-REORGANIZAR] Trend {trend_id} não tem posts")
                    continue
                
                # Converter para ObjectId para consulta
                for post_id in post_ids:
                    try:
                        obj_id = ObjectId(post_id)
                        all_post_ids.append(obj_id)
                        
                        # Mapear este post_id para esta trend
                        if post_id not in post_id_to_trend_map:
                            post_id_to_trend_map[post_id] = []
                        post_id_to_trend_map[post_id].append(trend_id)
                    except Exception as e:
                        logger.warning(f"[TRENDS-REORGANIZAR] ID de post inválido: {post_id}, erro: {e}")
            
            # Remover duplicatas - um post pode estar em múltiplas trends
            unique_post_ids = list(set(all_post_ids))
            if not unique_post_ids:
                logger.warning(f"[TRENDS-REORGANIZAR] Nenhum ID de post válido encontrado no lote atual")
                offset += batch_size
                continue
            
            # Buscar todos os posts com datas em uma única consulta
            logger.info(f"[TRENDS-REORGANIZAR] Buscando {len(unique_post_ids)} posts únicos")
            posts_with_dates = list(posts_coll.find(
                {"_id": {"$in": unique_post_ids}},
                {"_id": 1, "created_at": 1}
            ))
            
            # Criar dicionário post_id -> created_at
            post_dates = {}
            for post in posts_with_dates:
                post_id = str(post["_id"])
                created_at = post.get("created_at")
                if created_at:
                    post_dates[post_id] = created_at
            
            logger.info(f"[TRENDS-REORGANIZAR] Obtidas datas para {len(post_dates)} posts")
            
            # Organizar posts por trend e ordenar
            trends_data = {}
            for trend in batch:
                trend_id = trend["_id"]
                post_ids = trend.get("postIds", [])
                
                # Filtrar apenas posts que temos data
                valid_posts = [(pid, post_dates.get(pid)) for pid in post_ids if pid in post_dates]
                
                if not valid_posts:
                    logger.warning(f"[TRENDS-REORGANIZAR] Trend {trend_id} não tem posts com datas válidas")
                    continue
                
                # Ordenar posts por data (mais recente primeiro)
                sorted_posts = sorted(valid_posts, key=lambda x: x[1] if x[1] else datetime.min, reverse=True)
                
                # Pegar IDs ordenados e a data mais recente
                ordered_post_ids = [p[0] for p in sorted_posts]
                newest_date = sorted_posts[0][1] if sorted_posts else None
                
                # Armazenar para atualização
                trends_data[trend_id] = {
                    "ordered_post_ids": ordered_post_ids,
                    "newest_date": newest_date
                }
            
            # Função para processar cada trend em paralelo
            def process_trend(trend_id):
                try:
                    if trend_id not in trends_data:
                        return {"success": False, "trend_id": trend_id, "reason": "No data"}
                    
                    data = trends_data[trend_id]
                    ordered_post_ids = data["ordered_post_ids"]
                    newest_date = data["newest_date"]
                    
                    # Criar operação de atualização
                    update_fields = {
                        "postIds": ordered_post_ids
                    }
                    
                    # Atualizar updated_at apenas se temos data do post mais recente
                    if newest_date:
                        update_fields["updated_at"] = newest_date
                    
                    # Calcular tempo relativo para o campo lastUpdated (ex: "2 hours ago")
                    if newest_date:
                        update_fields["lastUpdated"] = format_time_ago(newest_date)
                    
                    # Executar atualização
                    result = trends_coll.update_one(
                        {"_id": trend_id},
                        {"$set": update_fields}
                    )
                    
                    return {
                        "success": result.modified_count > 0,
                        "trend_id": trend_id,
                        "modified": result.modified_count
                    }
                except Exception as e:
                    logger.error(f"[TRENDS-REORGANIZAR] Erro ao processar trend {trend_id}: {str(e)}")
                    return {"success": False, "trend_id": trend_id, "error": str(e)}
            
            # Processar trends em paralelo
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                trend_ids = list(trends_data.keys())
                results = list(executor.map(process_trend, trend_ids))
            
            # Processar resultados
            batch_success = sum(1 for r in results if r.get("success", False))
            batch_errors = len(results) - batch_success
            
            processed_count += len(results)
            update_count += batch_success
            error_count += batch_errors
            
            logger.info(f"[TRENDS-REORGANIZAR] Lote processado: {batch_success} trends atualizadas, {batch_errors} erros")
            
            # Avançar para o próximo lote
            offset += batch_size
        
        # Calcular estatísticas finais
        end_time = time.time()
        elapsed_time = end_time - start_time
        minutes = int(elapsed_time // 60)
        seconds = elapsed_time % 60
        
        logger.info(f"[TRENDS-REORGANIZAR] Processamento concluído em {minutes}m {seconds:.2f}s")
        logger.info(f"[TRENDS-REORGANIZAR] Total processado: {processed_count} trends")
        logger.info(f"[TRENDS-REORGANIZAR] Atualizadas com sucesso: {update_count} trends")
        logger.info(f"[TRENDS-REORGANIZAR] Erros: {error_count} trends")
        
        return {
            "total": total_trends,
            "processed": processed_count,
            "success": update_count,
            "errors": error_count,
            "elapsed_time": elapsed_time
        }
    
    except Exception as e:
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.error(f"[TRENDS-REORGANIZAR] ERRO CRÍTICO: {str(e)}")
        logger.error(f"[TRENDS-REORGANIZAR] Traceback: {traceback.format_exc()}")
        
        return {
            "success": False,
            "error": str(e),
            "elapsed_time": elapsed_time
        }
