import logging
import asyncio
import traceback
from datetime import datetime
import json

# from services.posts_services import clustering_posts
from services.clusters_services import process_clusters, clustering_posts
from services.trends_services import update_trends
from fastapi.concurrency import run_in_threadpool

logger = logging.getLogger(__name__)

async def cluster_pipeline_scheduler():
    """
    Função assíncrona que executa o pipeline de clustering a cada hora.
    Realiza:
    1. Clustering de posts
    2. Processamento de clusters 
    3. Atualização das trends
    """
    logger.info("Iniciando o agendador do pipeline de clustering")
    
    # Intervalo em segundos entre execuções (1 hora = 3600 segundos)
    INTERVAL = 1800
    
    while True:
        try:
            current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"[CLUSTER-SCHEDULER] Executando pipeline de clustering agendado em {current_time}")
            
            # Executar o pipeline de clustering
            logger.info(f"[CLUSTER-SCHEDULER] Chamando função run_cluster_pipeline()")
            result = await run_cluster_pipeline()
            
            logger.info(f"[CLUSTER-SCHEDULER] Execução do pipeline de clustering concluída com sucesso. Resultados: {json.dumps(result, ensure_ascii=False)}")
        except Exception as e:
            logger.error(f"[CLUSTER-SCHEDULER] ERRO na execução agendada do pipeline de clustering: {str(e)}")
            # Log do traceback completo para melhor diagnóstico
            logger.error(f"[CLUSTER-SCHEDULER] Traceback completo: {traceback.format_exc()}")
        
        # Aguardar o intervalo configurado antes da próxima execução
        logger.info(f"[CLUSTER-SCHEDULER] Próxima execução agendada para daqui a {INTERVAL//60} minutos")
        try:
            await asyncio.sleep(INTERVAL)
            logger.info(f"[CLUSTER-SCHEDULER] Sleep concluído, iniciando nova execução")
        except Exception as e:
            logger.error(f"[CLUSTER-SCHEDULER] ERRO durante sleep: {str(e)}")

async def run_cluster_pipeline():
    """
    Executa o pipeline completo de clustering de forma assíncrona.
    1. Agrupamento de posts (clustering)
    2. Processamento dos clusters para extrair informações
    3. Atualização das trends baseadas nos clusters
    
    Returns:
        dict: Resultado da execução com estatísticas
    """
    logger.info(f"[CLUSTER-PIPELINE] Iniciando execução completa do pipeline de clustering")
    try:
        # Etapa 1: Clustering dos posts
        start_time = datetime.now()
        logger.info("[CLUSTER-PIPELINE] Iniciando clustering de posts...")
        
        try:
            # Executar clustering_posts em uma thread separada para não bloquear o loop de eventos
            logger.info("[CLUSTER-PIPELINE] Chamando função clustering_posts via run_in_threadpool")
            await run_in_threadpool(clustering_posts)
            logger.info("[CLUSTER-PIPELINE] Função clustering_posts concluída com sucesso")
        except Exception as e:
            logger.error(f"[CLUSTER-PIPELINE] ERRO durante clustering_posts: {str(e)}")
            logger.error(f"[CLUSTER-PIPELINE] Traceback: {traceback.format_exc()}")
            raise
        
        clustering_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"[CLUSTER-PIPELINE] Clustering de posts concluído em {clustering_time:.2f} segundos")
        
        # Etapa 2: Processamento dos clusters gerados
        start_time = datetime.now()
        logger.info("[CLUSTER-PIPELINE] Iniciando processamento de clusters...")
        
        try:
            # Executar process_clusters em uma thread separada
            logger.info("[CLUSTER-PIPELINE] Chamando função process_clusters via run_in_threadpool")
            await run_in_threadpool(process_clusters)
            logger.info("[CLUSTER-PIPELINE] Função process_clusters concluída com sucesso")
        except Exception as e:
            logger.error(f"[CLUSTER-PIPELINE] ERRO durante process_clusters: {str(e)}")
            logger.error(f"[CLUSTER-PIPELINE] Traceback: {traceback.format_exc()}")
            raise
        
        processing_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"[CLUSTER-PIPELINE] Processamento de clusters concluído em {processing_time:.2f} segundos")
        
        # Etapa 3: Atualização das trends baseadas nos clusters processados
        start_time = datetime.now()
        logger.info("[CLUSTER-PIPELINE] Atualizando trends...")
        
        try:
            # Executar update_trends em uma thread separada
            logger.info("[CLUSTER-PIPELINE] Chamando função update_trends via run_in_threadpool")
            trends_result = await run_in_threadpool(update_trends)
            logger.info("[CLUSTER-PIPELINE] Função update_trends concluída com sucesso")
        except Exception as e:
            logger.error(f"[CLUSTER-PIPELINE] ERRO durante update_trends: {str(e)}")
            logger.error(f"[CLUSTER-PIPELINE] Traceback: {traceback.format_exc()}")
            raise
        
        trends_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"[CLUSTER-PIPELINE] Atualização de trends concluída em {trends_time:.2f} segundos")
        
        # Preparar resultado da execução completa
        result = {
            "status": "success",
            "execution_time": {
                "clustering": f"{clustering_time:.2f}s",
                "processing": f"{processing_time:.2f}s",
                "trends": f"{trends_time:.2f}s",
                "total": f"{(clustering_time + processing_time + trends_time):.2f}s"
            },
            "trends_result": trends_result
        }
        
        logger.info(f"[CLUSTER-PIPELINE] Pipeline completo finalizado com sucesso em {(clustering_time + processing_time + trends_time):.2f} segundos")
        return result
    
    except Exception as e:
        logger.error(f"[CLUSTER-PIPELINE] ERRO CRÍTICO na execução do pipeline de clustering: {str(e)}")
        logger.error(f"[CLUSTER-PIPELINE] Traceback completo: {traceback.format_exc()}")
        return {
            "status": "error",
            "error": str(e),
            "error_type": type(e).__name__,
            "traceback": traceback.format_exc()
        }

# Função principal para ser chamada externamente (via endpoint)
async def run_clustering_pipeline():
    """
    Função para executar o pipeline de clustering sob demanda.
    """
    logger.info("[CLUSTER-API] Iniciando execução do pipeline de clustering via API")
    try:
        result = await run_cluster_pipeline()
        logger.info(f"[CLUSTER-API] Pipeline executado com sucesso via API: {json.dumps(result, ensure_ascii=False)}")
        return result
    except Exception as e:
        logger.error(f"[CLUSTER-API] ERRO ao executar pipeline de clustering via API: {str(e)}")
        logger.error(f"[CLUSTER-API] Traceback: {traceback.format_exc()}")
        return {
            "success": False, 
            "message": f"Erro ao executar pipeline de clustering: {str(e)}"
        } 