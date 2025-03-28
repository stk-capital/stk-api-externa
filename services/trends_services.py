from util.mongodb_utils import get_mongo_collection
from env import db_name_stkfeed
import logging
from datetime import datetime, timedelta
from bson.objectid import ObjectId
import re

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
    logger.info("Iniciando geração de trends a partir de clusters...")
    
    # Conectar às coleções
    clusters_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="clusters")
    trends_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="trends")
    
    # Limpar coleção de trends existentes
    trends_coll.delete_many({})
    logger.info("Coleção de trends limpa")
    
    # Buscar todos os clusters processados com score de relevância >= 0.5
    processed_clusters = list(clusters_coll.find(
        {"was_processed": True, "relevance_score": {"$gte": 0.5}},
        {"_id": 1, "theme": 1, "summary": 1, "posts_ids": 1, "key_points": 1, 
         "relevance_score": 1, "dispersion_score": 1, "newest_post_date": 1,
         "stakeholder_impact": 1, "sector_specific": 1}
    ))
    
    if not processed_clusters:
        logger.info("Nenhum cluster processado encontrado para gerar trends")
        return
    
    logger.info(f"Encontrados {len(processed_clusters)} clusters processados para gerar trends")
    
    # Disclaimer padrão
    default_disclaimer = "This story is a summary of posts and may evolve over time."
    
    # Processar cada cluster e transformar em trend
    trends = []
    for cluster in processed_clusters:
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
                        newest_date = datetime.now()
            last_updated = format_time_ago(newest_date)
        
        #buld summary based on key ponits, risks and opportunities, format nicely
        summary = cluster.get("summary", "")
        if cluster.get("key_points"):
            summary += "\n\nKey Points:\n"
            for point in cluster.get("key_points"):
                summary += f"- {point}\n"
        if cluster.get("sector_specific"):
            summary += "\nRisks:\n"
            for risk in cluster.get("sector_specific", {}).get("risks", []):
                summary += f"- {risk}\n"
            summary += "\nOpportunities:\n"
            for opportunity in cluster.get("sector_specific", {}).get("opportunities", []):
                summary += f"- {opportunity}\n"
   
        

        # Criar trend
        trend = {
            "title": cluster.get("theme", "Untitled Trend"),
            "category": category,
            "posts": len(cluster.get("posts_ids", [])),
            "summary": summary,
            "lastUpdated": last_updated,
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
    
    # Inserir trends no banco de dados
    if trends:
        trends_coll.insert_many(trends)
        logger.info(f"Inseridas {len(trends)} trends na coleção")
    
    logger.info("Geração de trends concluída com sucesso")
    return len(trends)

# Função principal para ser chamada externamente
def update_trends():
    """
    Atualiza a coleção de trends a partir dos clusters processados.
    """
    try:
        num_trends = generate_trends_from_clusters()
        return {"success": True, "message": f"Trends atualizadas com sucesso. {num_trends} trends geradas."}
    except Exception as e:
        logger.error(f"Erro ao atualizar trends: {str(e)}")
        return {"success": False, "message": f"Erro ao atualizar trends: {str(e)}"} 
    