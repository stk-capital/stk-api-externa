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
    INTERVAL = 3600
    
    while True:
        try:
            current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"Executando pipeline de clustering agendado em {current_time}")
            
            # Executar o pipeline de clustering
            result = await run_cluster_pipeline()
            
            logger.info(f"Execução do pipeline de clustering concluída com sucesso. Resultados: {json.dumps(result, ensure_ascii=False)}")
        except Exception as e:
            logger.error(f"Erro na execução agendada do pipeline de clustering: {str(e)}")
            # Log do traceback completo para melhor diagnóstico
            logger.error(traceback.format_exc())
        
        # Aguardar o intervalo configurado antes da próxima execução
        logger.info(f"Próxima execução agendada para daqui a {INTERVAL//60} minutos")
        await asyncio.sleep(INTERVAL)

async def run_cluster_pipeline():
    """
    Executa o pipeline completo de clustering de forma assíncrona.
    1. Agrupamento de posts (clustering)
    2. Processamento dos clusters para extrair informações
    3. Atualização das trends baseadas nos clusters
    
    Returns:
        dict: Resultado da execução com estatísticas
    """
    try:
        # Etapa 1: Clustering dos posts
        start_time = datetime.now()
        logger.info("Iniciando clustering de posts...")
        
        # Executar clustering_posts em uma thread separada para não bloquear o loop de eventos
        await run_in_threadpool(clustering_posts)
        
        clustering_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Clustering de posts concluído em {clustering_time:.2f} segundos")
        
        # Etapa 2: Processamento dos clusters gerados
        start_time = datetime.now()
        logger.info("Iniciando processamento de clusters...")
        
        # Executar process_clusters em uma thread separada
        await run_in_threadpool(process_clusters)
        
        processing_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Processamento de clusters concluído em {processing_time:.2f} segundos")
        
        # Etapa 3: Atualização das trends baseadas nos clusters processados
        start_time = datetime.now()
        logger.info("Atualizando trends...")
        
        # Executar update_trends em uma thread separada
        trends_result = await run_in_threadpool(update_trends)
        
        trends_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Atualização de trends concluída em {trends_time:.2f} segundos")
        
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
        
        return result
    
    except Exception as e:
        logger.error(f"Erro na execução do pipeline de clustering: {str(e)}")
        return {
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc()
        }

# Função principal para ser chamada externamente (via endpoint)
async def run_clustering_pipeline():
    """
    Função para executar o pipeline de clustering sob demanda.
    """
    try:
        result = await run_cluster_pipeline()
        return result
    except Exception as e:
        logger.error(f"Erro ao executar pipeline de clustering: {str(e)}")
        return {
            "success": False, 
            "message": f"Erro ao executar pipeline de clustering: {str(e)}"
        } 