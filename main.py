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

import logging


import os
from dotenv import load_dotenv
from graph_executor import execute_graph
from fastapi.staticfiles import StaticFiles
import shutil
import traceback
import base64
import json

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

load_dotenv()

app = FastAPI(
    title="STK API Externa",
    description="API para integração externa STK Capital",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json"
)

# api_app = FastAPI()
# app.mount("/api", api_app)

# CORS middleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite todas as origens
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
        pupulate_by_name = True
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

# API routes


@app.get("/api/llms", response_model=List[LLMModel])
async def get_llms():
    llms = await db.llms.find().to_list(1000)
    return [LLMModel(**{**llm, "_id": str(llm["_id"])}) for llm in llms]

@app.get("/api/agents/{agent_id}", response_model=AgentModel)
async def get_agent(agent_id: str):
    agent = await db.agents.find_one({"_id": ObjectId(agent_id)})
    if agent:
        return AgentModel(**agent)
    raise HTTPException(status_code=404, detail="Agent not found")

# API routes


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
        logger.debug(f"WebSocket connection established for graph {graph_id}")
        
        # Receive the initial message and files
        data = await websocket.receive_text()
        try:
            input_data = json.loads(data)
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON received: {data}")
            await websocket.send_json({"error": "Invalid JSON format"})
            return

        logger.debug(f"Received input data: {input_data}")

        initial_message = input_data.get('initial_message', '')
        files = input_data.get('files', [])

        graph = await db.graphs.find_one({"_id": ObjectId(graph_id)})
        if not graph:
            logger.error(f"Graph not found: {graph_id}")
            await websocket.send_json({"error": "Graph not found"})
            return

        graph_name = graph.get('name', 'Unnamed Graph')

        # Handle file uploads
        file_paths = []
        for file in files:
            file_path = f"/tmp/{file['name']}"
            with open(file_path, "wb") as buffer:
                buffer.write(base64.b64decode(file['content']))
            file_paths.append(file_path)
            logger.debug(f"Saved file: {file_path}")

        with trace(
            name=f"Graph: {graph_name}",
            run_type="chain",
            metadata={"graph_name": graph_name, "graph_id": str(graph_id)}
        ) as root_run:
            async for result in execute_graph(graph, initial_message, file_paths):
                logger.debug(f"Sending result: {result}")
                await websocket.send_text(result)

        logger.debug("Graph execution completed")
    except WebSocketDisconnect:
        logger.error("WebSocket disconnected")
    except Exception as e:
        logger.exception(f"Error during graph execution: {str(e)}")
        await websocket.send_json({"error": str(e)})
    finally:
        logger.debug("Closing WebSocket connection")
        await websocket.close()



@app.on_event("startup")
async def startup_event():
    asyncio.create_task(start_scheduler())


if __name__ == "__main__":
    import uvicorn
    import argparse

    port = int(os.getenv('BACKEND_PORT', 8000))

    reload = os.environ.get("ENVIRONMENT", "development") == "development"

    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=reload)
