from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
from pydantic import BaseModel, Field, ConfigDict, ValidationError, model_validator
from typing import List, Optional, Dict, Any, Union
from fastapi import WebSocket, WebSocketDisconnect
from datetime import datetime
from scheduler import run_scheduled_graphs, execute_schedule_now, start_scheduler, calculate_next_run
import asyncio
from langsmith import trace
from email_processor import _process_emails, _process_chunks, _create_users_from_companies, _create_posts_from_infos, _log_processing_summary
from event_processing import _process_events    
import logging



import os
from dotenv import load_dotenv
from graph_executor import execute_graph
from fastapi.staticfiles import StaticFiles
import shutil
import traceback
import base64
import json
import sys
# Import for running sync functions in a threadpool
from fastapi.concurrency import run_in_threadpool


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

# api_app = FastAPI()
# app.mount("/api", api_app)

# CORS middleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount the /tmp directory to serve images
app.mount("/api/images/tmp", StaticFiles(directory="/tmp"), name="images")
# Serve frontend static files
# app.mount("/", StaticFiles(directory="../frontend/.next", html=True), name="frontend")

# MongoDB connection
client = AsyncIOMotorClient(os.getenv("MONGO_DB_URL"))
db = client.crewai_db


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


class LLMModel(BaseModel):
    id: Optional[str] = Field(alias="_id")
    model: str
    apiKey: str
    baseURL: str

    model_config = ConfigDict(
        populate_by_name=True,
        json_encoders={ObjectId: str},
        json_schema_extra={
            "example": {
                "model": "gpt-3.5-turbo",
                "apiKey": "your-api-key-here",
                "baseURL": "https://api.openai.com/v1"
            }
        }
    )

    @classmethod
    def from_mongo(cls, data):
        if not data:
            return None
        id = str(data.pop('_id', None))
        return cls(id=id, **data)


class AgentModel(BaseModel):
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    name: str
    role: str
    goal: str
    backstory: str
    llm: str
    temperature: Optional[float] = Field(ge=0, le=1)

    class Config:
        populate_by_name = True
        json_encoders = {ObjectId: str}
        arbitrary_types_allowed = True


class ToolConfig(BaseModel):
    api_keys: Optional[Dict[str, str]] = {}
    arguments: Optional[Dict[str, str]] = {}
    custom_function: Optional[str] = ''


class Tool(BaseModel):
    id: Optional[str] = Field(alias="_id", default=None)
    name: str
    description: str
    type: str
    module: Optional[str] = None
    dependencies: Union[str, List[str]] = Field(default="")
    config: ToolConfig = Field(default_factory=ToolConfig)

    class Config:
        populate_by_name = True
        json_encoders = {ObjectId: str}

    @classmethod
    def from_mongo(cls, data):
        if not data:
            return None
        # Ensure _id is converted to string and set as id
        id = str(data.pop('_id', None))
        return cls(id=id, **data)


class Node(BaseModel):
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    type: str  # 'agent' or 'tool'
    reference_id: str  # ID of the agent or tool
    position: Dict[str, int]  # {x: int, y: int}


class Edge(BaseModel):
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    source: str  # Node ID
    target: str  # Node ID

# Update the Graph model


class Graph(BaseModel):
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    name: str
    # Changed from List[Node] to List[Dict[str, Any]]
    nodes: List[Dict[str, Any]]
    # Changed from List[Edge] to List[Dict[str, Any]]
    edges: List[Dict[str, Any]]

    class Config:
        populate_by_name = True
        json_encoders = {ObjectId: str}

    @classmethod
    def from_mongo(cls, data):
        if not data:
            return None
        # Ensure _id is converted to string and set as id
        id = str(data.pop('_id', None))
        return cls(id=id, **data)


class Schedule(BaseModel):
    id: Optional[PyObjectId] = Field(default=None, alias="_id")
    graph_id: Union[str, PyObjectId]
    initial_input: str
    date: str
    time: str
    frequency: str
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None

    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}

    @model_validator(mode='before')
    @classmethod
    def validate_graph_id(cls, values):
        graph_id = values.get('graph_id')
        if isinstance(graph_id, ObjectId):
            values['graph_id'] = str(graph_id)
        return values


def process_full_pipeline(process_emails_count=10):


    _process_emails(process_emails_count)
    _process_chunks()
    _create_users_from_companies()
    _create_posts_from_infos()
    _process_events()
    results = _log_processing_summary(datetime.now() - timedelta(minutes=5))

    #delete all emails created after 2025-03-03

    return results

# API routes


@app.get("/api/llms", response_model=List[LLMModel])
async def get_llms():
    llms = await db.llms.find().to_list(1000)
    return [LLMModel(**{**llm, "_id": str(llm["_id"])}) for llm in llms]


@app.get("/api/agents", response_model=List[AgentModel])
async def get_agents():
    agents = await db.agents.find().to_list(1000)
    return [AgentModel(**{**agent, "_id": str(agent["_id"]), "temperature": agent.get('temperature', 0.7)}) for agent in agents]


@app.get("/api/agents/{agent_id}", response_model=AgentModel)
async def get_agent(agent_id: str):
    agent = await db.agents.find_one({"_id": ObjectId(agent_id)})
    if agent:
        return AgentModel(**agent)
    raise HTTPException(status_code=404, detail="Agent not found")

# API rout

@app.get("/api/tools", response_model=List[Tool])
async def get_tools():
    tools = await db.tools.find().to_list(1000)

    return [Tool(**{**tool, "_id": str(tool["_id"])}) for tool in tools]



@app.get("/api/tools/{tool_id}", response_model=Tool)
async def get_tool(tool_id: str):
    tool = await db.tools.find_one({"_id": ObjectId(tool_id)})
    if tool:
        return Tool.from_mongo(tool)
    raise HTTPException(status_code=404, detail="Tool not found")



@app.delete("/api/tools/{tool_id}")
async def delete_tool(tool_id: str):
    delete_result = await db.tools.delete_one({"_id": ObjectId(tool_id)})
    if delete_result.deleted_count == 1:
        return {"message": "Tool deleted successfully"}
    raise HTTPException(status_code=404, detail="Tool not found")

# Update or add these routes for graph management
@app.get("/api/graphs", response_model=List[Graph])
async def get_graphs():
    graphs = await db.graphs.find().to_list(1000)
    # print("Graphs:", graphs)
    return [Graph(**{**graph, "_id": str(graph["_id"])}) for graph in graphs]


@app.get("/api/graphs/{graph_id}", response_model=Graph)
async def get_graph(graph_id: str):
    graph = await db.graphs.find_one({"_id": ObjectId(graph_id)})
    if graph:
        return Graph.from_mongo(graph)
    raise HTTPException(status_code=404, detail="Graph not found")

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

        # Gets the value of the 'name' key; if there's none, assigns 'Unnamed Graph' as value
        graph_name = graph.get('name', 'Unnamed Graph')

        # logger.debug(f"Executing graph: {graph}")
        with trace(
            name=f"Graph: {graph_name}",
            run_type="chain",
            metadata={"graph_name": graph_name, "graph_id": str(graph_id)}
        ) as root_run:
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

# backend/main.py



@app.get("/api/schedules", response_model=List[Schedule])
async def get_schedules():
    schedules = await db.schedules.find().to_list(1000)
    return [Schedule(**schedule) for schedule in schedules]



@app.get("/api/schedules/{schedule_id}/logs")
async def get_execution_logs(schedule_id: str):
    logs = await db.execution_logs.find({"schedule_id": ObjectId(schedule_id)}).sort("execution_time", -1).to_list(10)
    return [{"execution_time": log["execution_time"], "log": log.get("log"), "error": log.get("error")} for log in logs]


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
    import argparse

    port = int(os.getenv('BACKEND_PORT', 8000))

    reload = os.environ.get("ENVIRONMENT", "development") == "development"

    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=reload)
