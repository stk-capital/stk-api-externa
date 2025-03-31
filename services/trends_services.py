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
    Gera trends a partir dos clusters processados e os salva na coleção 'trends'.
    """
    logger.info("[TRENDS] Iniciando geração de trends a partir de clusters...")
    
    try:
        # Conectar às coleções
        logger.info("[TRENDS] Conectando às coleções no MongoDB")
        clusters_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="clusters")
        trends_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="trends")
        
        # Limpar coleção de trends existentes
        logger.info("[TRENDS] Limpando coleção de trends existentes")
        delete_result = trends_coll.delete_many({})
        logger.info(f"[TRENDS] {delete_result.deleted_count} trends anteriores foram removidas")
        
        # Buscar todos os clusters processados com score de relevância >= 0.5
        logger.info("[TRENDS] Buscando clusters processados com relevance_score >= 0.5")
        processed_clusters = list(clusters_coll.find(
            {"was_processed": True, "relevance_score": {"$gte": 0.2}},
            {"_id": 1, "theme": 1, "summary": 1, "posts_ids": 1, "key_points": 1, 
             "relevance_score": 1, "dispersion_score": 1, "newest_post_date": 1,
             "stakeholder_impact": 1, "sector_specific": 1}
        ))
        
        if not processed_clusters:
            logger.warning("[TRENDS] Nenhum cluster processado encontrado para gerar trends")
            return 0
        
        logger.info(f"[TRENDS] Encontrados {len(processed_clusters)} clusters processados para gerar trends")
        
        # Disclaimer padrão
        default_disclaimer = "This story is a summary of posts and may evolve over time."
        
        # Processar cada cluster e transformar em trend
        trends = []
        
        for idx, cluster in enumerate(processed_clusters):
            logger.info(f"[TRENDS] Processando cluster {idx+1}/{len(processed_clusters)}: {cluster['_id']}")
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
                
                #buld summary based on key ponits, risks and opportunities, format nicely
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
                
                trends.append(trend)
                logger.info(f"[TRENDS] Trend '{trend['title']}' criada com {trend['posts']} posts")
                
            except Exception as e:
                logger.error(f"[TRENDS] ERRO ao processar cluster {cluster['_id']}: {str(e)}")
                logger.error(f"[TRENDS] Traceback: {traceback.format_exc()}")
                # Continua para o próximo cluster mesmo se houver erro
        
        # Inserir trends no banco de dados
        if trends:
            logger.info(f"[TRENDS] Inserindo {len(trends)} trends na coleção")
            insert_result = trends_coll.insert_many(trends)
            logger.info(f"[TRENDS] {len(insert_result.inserted_ids)} trends inseridas com sucesso")
        else:
            logger.warning("[TRENDS] Nenhuma trend foi criada para inserção")
        
        logger.info("[TRENDS] Geração de trends concluída com sucesso")
        return len(trends)
    
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
    