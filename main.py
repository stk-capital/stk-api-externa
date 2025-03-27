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
from services.events_services import _process_events
from datetime import timedelta

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
    _process_events()
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

if __name__ == "__main__":

    import uvicorn
    port = int(os.getenv('BACKEND_PORT', 8000))
    reload = os.environ.get("ENVIRONMENT", "development") == "development"
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=reload)
