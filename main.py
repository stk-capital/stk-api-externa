from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from bson import ObjectId
from pydantic import BaseModel, Field, ConfigDict, model_validator
from typing import List, Optional, Dict, Any, Union
from fastapi import WebSocket, WebSocketDisconnect
from datetime import datetime
from scheduler import start_scheduler
import asyncio

from util.logs_utils import _log_processing_summary
from services.emails_services import _process_emails
from services.chunks_services import _process_chunks
from services.users_services import _process_users
from services.posts_services import _process_posts
from services.trends_services import update_trends
from datetime import timedelta
from services.clustering_scheduler import cluster_pipeline_scheduler, run_clustering_pipeline

import logging
import os
from dotenv import load_dotenv
from services.langchain_services import execute_graph
from fastapi.staticfiles import StaticFiles
import traceback
import json
import sys
# Import for running sync functions in a threadpool
from fastapi.concurrency import run_in_threadpool
from util.mongodb_utils import get_async_database
from util.users_utils import get_company_logo
from scripts.update_company_avatars import get_priority_companies, update_company_avatars


# Configure logging properly for Azure App Service
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(stream=sys.stdout)  # Force logs to stdout instead of stderr
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

# Added environment flag for development mode
DEVELOPMENT_MODE = os.getenv("DEVELOPMENT_MODE", "false").lower() == "true"

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount the /tmp directory to serve images
app.mount("/api/images/tmp", StaticFiles(directory="/tmp"), name="images")
db = get_async_database("crewai_db")

class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v, *args):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid objectid")
        return ObjectId(v)

    @classmethod
    def __get_pydantic_json_schema__(cls, field_schema: Dict[str, Any]) -> None:
        field_schema.update(type="string")

def process_full_pipeline(process_emails_count=10):

    _process_emails(process_emails_count)
    _process_chunks()
    _process_users()
    _process_posts()
    # _process_events()
    
    # Notar que clustering_posts, process_clusters e update_trends
    # agora são executados no pipeline de clustering separado
    
    results = _log_processing_summary(datetime.now() - timedelta(minutes=5))

    #delete all emails created after 2025-03-03

    return results

# API routes

@app.websocket("/api/graphs/{graph_id}/execute")
async def execute_graph_ws(websocket: WebSocket, graph_id: str):
    await websocket.accept()
    try:
        # logger.debug(f"WebSocket connection established for graph {graph_id}")
        input_data = await websocket.receive_json()
        # logger.debug(f"Received input data: {input_data}")

        graph = await db.graphs.find_one({"_id": ObjectId(graph_id)})
        if not graph:
            logger.error(f"Graph not found: {graph_id}")
            await websocket.send_json({"error": "Graph not found"})
            return

 
        async for result in execute_graph(graph, input_data['initial_message']):
            logger.debug(f"Sending result: {result}")
            # Send as text since it's already JSON-encoded
            await websocket.send_text(result)

        # logger.debug("Graph execution completed")
    except WebSocketDisconnect:
        logger.error("WebSocket disconnected")
    except Exception as e:
        # logger.exception(f"Error during graph execution: {str(e)}")
        await websocket.send_json({"error": str(e)})
    finally:
        # logger.debug("Closing WebSocket connection")
        await websocket.close()


# Agendador para o endpoint de pipeline
async def pipeline_scheduler():
    """
    Função assíncrona que executa o endpoint de pipeline a cada 5 minutos.
    """
    logger.info("Iniciando o agendador do endpoint de pipeline")
    while True:
        try:
            current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"Executando pipeline agendado em {current_time}")
            result = await run_in_threadpool(process_full_pipeline, 10)  # Processa 10 emails por padrão
            logger.info(f"Execução do pipeline concluída com sucesso. Resultados: {json.dumps(result, ensure_ascii=False)}")
        except Exception as e:
            logger.error(f"Erro na execução agendada do pipeline: {str(e)}")
            # Log the full traceback for better debugging
            logger.error(traceback.format_exc())
        
        # Wait for 5 minutes before next execution
        await asyncio.sleep(300)  # 5 minutos = 300 segundos

@app.on_event("startup")
async def startup_event():
    # Inicia o agendador de grafos apenas se não estiver em modo de desenvolvimento
    if not DEVELOPMENT_MODE:
        logger.info("Starting scheduler - Production mode")
        asyncio.create_task(start_scheduler())
        # Inicia o agendador do endpoint de pipeline
        asyncio.create_task(pipeline_scheduler())
        # Inicia o agendador do pipeline de clustering (executa a cada hora)
        logger.info("[STARTUP] Iniciando agendador do pipeline de clustering")
        try:
            task = asyncio.create_task(cluster_pipeline_scheduler())
            logger.info("[STARTUP] Agendador do pipeline de clustering iniciado com sucesso")
        except Exception as e:
            logger.error(f"[STARTUP] ERRO ao iniciar agendador do pipeline de clustering: {str(e)}")
            logger.error(f"[STARTUP] Traceback: {traceback.format_exc()}")
    else:
        logger.info("Scheduler disabled - Development mode")
    logger.info("Agendadores iniciados com sucesso")

# email processor process_full_pipeline
@app.post("/api/pipeline")
async def run_pipeline(process_emails_count: int = 10):
    """
    Executes the full pipeline defined in process_full_pipeline.
    The synchronous function is run in a threadpool to avoid blocking the async event loop.
    """
    try:
        result = await run_in_threadpool(process_full_pipeline, process_emails_count)
        return result
    except Exception as e:
        logger.error(f"Pipeline execution error: {e}")
        raise HTTPException(status_code=500, detail="Error executing pipeline")

@app.post("/api/trends/update")
async def update_trends_endpoint():
    """
    Atualiza a coleção de trends a partir dos clusters processados.
    """
    try:
        logger.info("[API] Iniciando atualização de trends via endpoint")
        result = await run_in_threadpool(update_trends)
        logger.info(f"[API] Atualização de trends concluída: {json.dumps(result, ensure_ascii=False)}")
        return result
    except Exception as e:
        logger.error(f"[API] Erro na atualização de trends: {str(e)}")
        logger.error(f"[API] Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Error updating trends: {str(e)}")

@app.post("/api/clustering/run")
async def run_clustering_endpoint():
    """
    Executa o pipeline completo de clustering sob demanda:
    1. Clustering de posts
    2. Processamento de clusters
    3. Atualização de trends
    """
    try:
        logger.info("[API] Iniciando pipeline de clustering via endpoint")
        result = await run_clustering_pipeline()
        logger.info(f"[API] Pipeline de clustering concluído: {json.dumps(result, ensure_ascii=False)}")
        return result
    except Exception as e:
        logger.error(f"[API] Erro no pipeline de clustering: {str(e)}")
        logger.error(f"[API] Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Error executing clustering pipeline: {str(e)}")

# Variável global para armazenar o status da atualização
avatar_update_status = {
    "is_running": False,
    "start_time": None,
    "end_time": None,
    "total": 0,
    "processed": 0,
    "success": 0,
    "failed": 0
}

@app.post("/api/users/update-avatars")
async def update_users_avatars(limit: int = 400, batch_size: int = 50):
    """
    Atualiza os avatares dos usuários existentes usando a API Clearbit.
    Foca nas empresas mais importantes com base em métricas de engajamento.
    
    Args:
        limit: Número máximo de empresas para processar (padrão: 400)
        batch_size: Tamanho do lote de processamento (padrão: 50)
    """
    global avatar_update_status
    
    # Verificar se já está em execução
    if avatar_update_status["is_running"]:
        return {
            "message": "Uma atualização de avatares já está em andamento",
            "status": avatar_update_status
        }
        
    try:
        # Marcar como em execução
        avatar_update_status = {
            "is_running": True,
            "start_time": datetime.now().isoformat(),
            "end_time": None,
            "total": 0,
            "processed": 0,
            "success": 0,
            "failed": 0
        }
        
        # Executar em threadpool para não bloquear
        priority_companies = await run_in_threadpool(get_priority_companies, limit=limit)
        avatar_update_status["total"] = len(priority_companies)
        
        # Função para executar e atualizar status
        async def execute_update():
            global avatar_update_status
            try:
                stats = await run_in_threadpool(
                    update_company_avatars, 
                    priority_companies, 
                    batch_size=batch_size
                )
                
                # Atualizar status final
                avatar_update_status.update({
                    "is_running": False,
                    "end_time": datetime.now().isoformat(),
                    "processed": stats["processed"],
                    "success": stats["success"],
                    "failed": stats["failed"]
                })
            except Exception as e:
                logger.error(f"Erro durante atualização de avatares: {str(e)}")
                avatar_update_status.update({
                    "is_running": False,
                    "end_time": datetime.now().isoformat(),
                    "error": str(e)
                })
        
        # Iniciar processamento em background
        asyncio.create_task(execute_update())
        
        return {
            "message": f"Atualização de avatares iniciada para {len(priority_companies)} empresas",
            "companies_count": len(priority_companies),
            "status": "processing"
        }
    except Exception as e:
        # Resetar status em caso de erro
        avatar_update_status["is_running"] = False
        avatar_update_status["error"] = str(e)
        
        logger.error(f"Erro ao iniciar atualização de avatares: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao iniciar atualização de avatares: {str(e)}")

@app.get("/api/users/avatar-update-status")
async def get_avatar_update_status():
    """
    Retorna o status atual da atualização de avatares.
    """
    global avatar_update_status
    
    # Calcular estatísticas adicionais se estiver em execução
    if avatar_update_status["is_running"] and avatar_update_status["total"] > 0:
        progress = (avatar_update_status["processed"] / avatar_update_status["total"]) * 100
        
        # Calcular tempo estimado para conclusão
        if avatar_update_status["processed"] > 0 and avatar_update_status["start_time"]:
            start_time = datetime.fromisoformat(avatar_update_status["start_time"])
            elapsed_seconds = (datetime.now() - start_time).total_seconds()
            rate = avatar_update_status["processed"] / elapsed_seconds
            remaining_items = avatar_update_status["total"] - avatar_update_status["processed"]
            
            if rate > 0:
                estimated_seconds_remaining = remaining_items / rate
                estimated_completion = (datetime.now() + timedelta(seconds=estimated_seconds_remaining)).isoformat()
                
                return {
                    **avatar_update_status,
                    "progress_percent": round(progress, 2),
                    "elapsed_seconds": round(elapsed_seconds, 2),
                    "estimated_completion": estimated_completion
                }
    
    return avatar_update_status

if __name__ == "__main__":

    import uvicorn
    port = int(os.getenv('BACKEND_PORT', 8000))
    reload = os.environ.get("ENVIRONMENT", "development") == "development"
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=reload)
