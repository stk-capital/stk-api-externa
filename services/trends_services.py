from util.mongodb_utils import get_mongo_collection
from env import db_name_stkfeed
import logging
from datetime import datetime, timedelta
from bson.objectid import ObjectId
import re
import traceback

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
    """
    logger.info("[TRENDS] Iniciando geração e atualização de trends a partir de clusters...")
    
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
        
        updated_trend_count = 0
        
        for cluster in updated_clusters:
            cluster_id = str(cluster["_id"])
            
            # Buscar trend existente para este cluster
            existing_trend = trends_coll.find_one({"cluster_id": cluster_id})
            
            if existing_trend:
                logger.info(f"[TRENDS] Atualizando trend existente para cluster: {cluster_id}")
                
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
                
                # Atualizar a trend existente
                trends_coll.update_one(
                    {"_id": existing_trend["_id"]},
                    {"$set": {
                        "title": cluster.get("theme", "Untitled Trend"),
                        "posts": len(cluster.get("posts_ids", [])),
                        "summary": summary,
                        "lastUpdated": last_updated,
                        "updated_at": datetime.utcnow(),
                        "postIds": cluster.get("posts_ids", []),
                        "key_points": cluster.get("key_points", []),
                        "relevance_score": cluster.get("relevance_score", 0),
                        "dispersion_score": cluster.get("dispersion_score", 0),
                        "stakeholder_impact": cluster.get("stakeholder_impact", ""),
                        "sector_specific": cluster.get("sector_specific", {"opportunities": [], "risks": []})
                    }}
                )
                updated_trend_count += 1
                logger.info(f"[TRENDS] Trend atualizada: '{cluster.get('theme', 'Untitled Trend')}' com {len(cluster.get('posts_ids', []))} posts")
        
        logger.info(f"[TRENDS] Total de {updated_trend_count} trends atualizadas")
        
        # 2. CRIAR NOVAS TRENDS PARA CLUSTERS SEM TRENDS
        logger.info("[TRENDS] Buscando clusters processados sem trends associadas")
        
        # Usar aggregation para encontrar clusters que não têm trends associadas
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
        logger.info(f"[TRENDS] Encontrados {len(new_clusters)} clusters sem trends associadas")
        
        # Criar novas trends para esses clusters
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
                logger.info(f"[TRENDS] Nova trend criada: '{trend['title']}' com {trend['posts']} posts")
                
            except Exception as e:
                logger.error(f"[TRENDS] ERRO ao processar cluster {cluster['_id']}: {str(e)}")
                logger.error(f"[TRENDS] Traceback: {traceback.format_exc()}")
                # Continua para o próximo cluster mesmo se houver erro
        
        # Inserir novas trends no banco de dados
        if new_trends:
            logger.info(f"[TRENDS] Inserindo {len(new_trends)} novas trends na coleção")
            insert_result = trends_coll.insert_many(new_trends)
            logger.info(f"[TRENDS] {len(insert_result.inserted_ids)} novas trends inseridas com sucesso")
        else:
            logger.warning("[TRENDS] Nenhuma nova trend foi criada para inserção")
        
        # 3. RESETAR FLAG WAS_UPDATED NOS CLUSTERS PROCESSADOS
        logger.info("[TRENDS] Resetando flag was_updated em clusters processados")
        update_result = clusters_coll.update_many(
            {"was_updated": True},
            {"$set": {"was_updated": False}}
        )
        logger.info(f"[TRENDS] Flag was_updated resetada em {update_result.modified_count} clusters")
        
        # RESULTADOS
        total_trends = updated_trend_count + len(new_trends)
        logger.info("[TRENDS] Geração e atualização de trends concluída com sucesso")
        logger.info(f"[TRENDS] Total: {total_trends} trends ({updated_trend_count} atualizadas, {len(new_trends)} novas)")
        
        return total_trends
    
    except Exception as e:
        logger.error(f"[TRENDS] ERRO CRÍTICO durante geração de trends: {str(e)}")
        logger.error(f"[TRENDS] Traceback completo: {traceback.format_exc()}")
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
        num_trends = generate_trends_from_clusters()
        
        execution_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"[TRENDS-UPDATE] Trends atualizadas com sucesso. {num_trends} trends geradas em {execution_time:.2f} segundos")
        
        return {
            "success": True, 
            "message": f"Trends atualizadas com sucesso. {num_trends} trends geradas.",
            "trends_count": num_trends,
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
    