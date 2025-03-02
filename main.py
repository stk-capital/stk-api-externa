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

app = FastAPI()

# api_app = FastAPI()
# app.mount("/api", api_app)

# CORS middleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://172.16.21.7:3000", "http://localhost:3000"],
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


@app.post("/api/llms", response_model=LLMModel)
async def create_llm(llm: LLMModel):
    llm_dict = llm.model_dump(exclude={"id"})
    new_llm = await db.llms.insert_one(llm_dict)
    created_llm = await db.llms.find_one({"_id": new_llm.inserted_id})
    return LLMModel(**{**created_llm, "_id": str(created_llm["_id"])})


@app.put("/api/llms/{llm_id}", response_model=LLMModel)
async def update_llm(llm_id: str, llm: LLMModel):
    llm_dict = llm.model_dump(exclude={"id"})
    updated_llm = await db.llms.find_one_and_update(
        {"_id": ObjectId(llm_id)},
        {"$set": llm_dict},
        return_document=True
    )
    if updated_llm:
        return LLMModel(**{**updated_llm, "_id": str(updated_llm["_id"])})
    raise HTTPException(status_code=404, detail="LLM not found")


@app.delete("/api/llms/{llm_id}")
async def delete_llm(llm_id: str):
    delete_result = await db.llms.delete_one({"_id": ObjectId(llm_id)})
    if delete_result.deleted_count == 1:
        return {"message": "LLM deleted successfully"}
    raise HTTPException(status_code=404, detail="LLM not found")

# Add similar routes for agents, tools, and crews


@app.get("/api/agents", response_model=List[AgentModel])
async def get_agents():
    agents = await db.agents.find().to_list(1000)
    return [AgentModel(**{**agent, "_id": str(agent["_id"]), "temperature": agent.get('temperature', 0.7)}) for agent in agents]


@app.post("/api/agents", response_model=AgentModel)
async def create_agent(agent: AgentModel):
    agent_dict = agent.model_dump(exclude={"id"}, exclude_none=True)
    new_agent = await db.agents.insert_one(agent_dict)
    created_agent = await db.agents.find_one({"_id": new_agent.inserted_id})
    return AgentModel(**{**created_agent, "_id": str(created_agent["_id"])})


@app.put("/api/agents/{agent_id}", response_model=AgentModel)
async def update_agent(agent_id: str, agent: AgentModel):
    try:
        # print(f"Received update request for agent {agent_id}")
        # print(f"Received data: {agent.model_dump(exclude={'id'})}")

        agent_dict = agent.model_dump(exclude={"id"}, exclude_unset=True)
        updated_agent = await db.agents.find_one_and_update(
            {"_id": ObjectId(agent_id)},
            {"$set": agent_dict},
            return_document=True
        )
        if updated_agent:
            return AgentModel(**updated_agent)
        raise HTTPException(status_code=404, detail="Agent not found")
    except Exception as e:
        # print(f"Error updating agent: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/agents/{agent_id}")
async def delete_agent(agent_id: str):
    delete_result = await db.agents.delete_one({"_id": ObjectId(agent_id)})
    if delete_result.deleted_count == 1:
        return {"message": "Agent deleted successfully"}
    raise HTTPException(status_code=404, detail="Agent not found")


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


@app.post("/api/tools", response_model=Tool)
async def create_tool(tool: Tool):
    try:

        tool_dict = tool.dict(exclude={"id"}, exclude_unset=True)
        new_tool = await db.tools.insert_one(tool_dict)
        created_tool = await db.tools.find_one({"_id": new_tool.inserted_id})
        return Tool.from_mongo(created_tool)
    except Exception as e:
        print(f"Error creating tool: {str(e)}")
        raise HTTPException(
            status_code=422, detail=f"Error creating tool: {str(e)}")

# get tool by id


@app.get("/api/tools/{tool_id}", response_model=Tool)
async def get_tool(tool_id: str):
    tool = await db.tools.find_one({"_id": ObjectId(tool_id)})
    if tool:
        return Tool.from_mongo(tool)
    raise HTTPException(status_code=404, detail="Tool not found")


@app.put("/api/tools/{tool_id}", response_model=Tool)
async def update_tool(tool_id: str, tool: Tool):
    try:

        tool_dict = tool.dict(exclude={"id"}, exclude_unset=True)
        updated_tool = await db.tools.find_one_and_update(
            {"_id": ObjectId(tool_id)},
            {"$set": tool_dict},
            return_document=True
        )
        if updated_tool:
            return Tool.from_mongo(updated_tool)
        raise HTTPException(status_code=404, detail="Tool not found")
    except Exception as e:
        print(f"Error updating tool: {str(e)}")
        raise HTTPException(
            status_code=422, detail=f"Error updating tool: {str(e)}")


@app.delete("/api/tools/{tool_id}")
async def delete_tool(tool_id: str):
    delete_result = await db.tools.delete_one({"_id": ObjectId(tool_id)})
    if delete_result.deleted_count == 1:
        return {"message": "Tool deleted successfully"}
    raise HTTPException(status_code=404, detail="Tool not found")


@app.post("/api/graphs", response_model=Graph)
async def create_graph(graph: Graph):
    graph_dict = graph.dict(exclude={"id"}, exclude_unset=True)
    new_graph = await db.graphs.insert_one(graph_dict)
    created_graph = await db.graphs.find_one({"_id": new_graph.inserted_id})
    return Graph.from_mongo(created_graph)

# Update or add these routes for graph management


@app.post("/api/graphs", response_model=Graph)
async def create_graph(graph: Graph):
    graph_dict = graph.dict(exclude={"id"}, exclude_unset=True)
    new_graph = await db.graphs.insert_one(graph_dict)
    created_graph = await db.graphs.find_one({"_id": new_graph.inserted_id})
    return Graph.from_mongo(created_graph)


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


@app.put("/api/graphs/{graph_id}", response_model=Graph)
async def update_graph(graph_id: str, graph: Graph):
    graph_dict = graph.dict(exclude={"id"}, exclude_unset=True)
    updated_graph = await db.graphs.find_one_and_update(
        {"_id": ObjectId(graph_id)},
        {"$set": graph_dict},
        return_document=True
    )
    if updated_graph:
        return Graph.from_mongo(updated_graph)
    raise HTTPException(status_code=404, detail="Graph not found")


@app.delete("/api/graphs/{graph_id}")
async def delete_graph(graph_id: str):
    delete_result = await db.graphs.delete_one({"_id": ObjectId(graph_id)})
    if delete_result.deleted_count == 1:
        return {"message": "Graph deleted successfully"}
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

# backend/main.py


@app.post("/api/schedules", response_model=Schedule)
async def create_schedule(schedule: Schedule):
    schedule_dict = schedule.dict(exclude={"id"}, exclude_unset=True)
    schedule_dict["graph_id"] = ObjectId(schedule_dict["graph_id"])
    schedule_dict["next_run"] = calculate_next_run(
        schedule_dict["frequency"], schedule_dict["date"], schedule_dict["time"])
    new_schedule = await db.schedules.insert_one(schedule_dict)
    created_schedule = await db.schedules.find_one({"_id": new_schedule.inserted_id})
    return Schedule(**created_schedule)


@app.get("/api/schedules", response_model=List[Schedule])
async def get_schedules():
    schedules = await db.schedules.find().to_list(1000)
    return [Schedule(**schedule) for schedule in schedules]


@app.put("/api/schedules/{schedule_id}", response_model=Schedule)
async def update_schedule(schedule_id: str, schedule: Schedule):
    try:
        schedule_dict = schedule.model_dump(exclude={"id"}, exclude_unset=True)
        if "graph_id" in schedule_dict:
            schedule_dict["graph_id"] = ObjectId(schedule_dict["graph_id"])
        if "date" in schedule_dict or "time" in schedule_dict or "frequency" in schedule_dict:
            existing = await db.schedules.find_one({"_id": ObjectId(schedule_id)})
            try:
                schedule_dict["next_run"] = calculate_next_run(
                    schedule_dict.get("frequency", existing["frequency"]),
                    schedule_dict.get("date", existing["date"]),
                    schedule_dict.get("time", existing["time"])
                )
            except ValueError as e:
                raise HTTPException(
                    status_code=400, detail=f"Invalid schedule data: {str(e)}")

        updated_schedule = await db.schedules.find_one_and_update(
            {"_id": ObjectId(schedule_id)},
            {"$set": schedule_dict},
            return_document=True
        )
        if updated_schedule:
            # Convert ObjectId to string for the response
            updated_schedule['_id'] = str(updated_schedule['_id'])
            updated_schedule['graph_id'] = str(updated_schedule['graph_id'])
            return Schedule(**updated_schedule)
        raise HTTPException(status_code=404, detail="Schedule not found")
    except Exception as e:
        logger.error(f"Error updating schedule: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"An error occurred while updating the schedule: {str(e)}")


@app.delete("/api/schedules/{schedule_id}")
async def delete_schedule(schedule_id: str):
    delete_result = await db.schedules.delete_one({"_id": ObjectId(schedule_id)})
    if delete_result.deleted_count == 1:
        return {"message": "Schedule deleted successfully"}
    raise HTTPException(status_code=404, detail="Schedule not found")


@app.post("/api/schedules/{schedule_id}/execute")
async def execute_schedule(schedule_id: str):
    try:
        logger.info(f"Attempting to execute schedule: {schedule_id}")
        execution_log = await execute_schedule_now(schedule_id)
        logger.info(f"Schedule {schedule_id} executed successfully")
        return {"message": "Schedule executed successfully", "log": execution_log}
    except ValueError as e:
        logger.error(f"ValueError in execute_schedule: {str(e)}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(
            f"Unexpected error in execute_schedule: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}")


@app.get("/api/schedules/{schedule_id}/logs")
async def get_execution_logs(schedule_id: str):
    logs = await db.execution_logs.find({"schedule_id": ObjectId(schedule_id)}).sort("execution_time", -1).to_list(10)
    return [{"execution_time": log["execution_time"], "log": log.get("log"), "error": log.get("error")} for log in logs]


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(start_scheduler())

@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        if not file:
            raise HTTPException(status_code=400, detail="No file provided")
        
        file_name = file.filename
        if not file_name:
            raise HTTPException(status_code=400, detail="File name is missing")

        # Ensure the /tmp directory exists
        os.makedirs("/tmp", exist_ok=True)

        file_path = f"/tmp/{file_name}"
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        # Check if the file was actually written
        if not os.path.exists(file_path):
            raise HTTPException(status_code=500, detail="File was not saved successfully")

        return {"message": "File uploaded successfully", "file_name": file_name}
    except Exception as e:
        logger.error(f"Error during file upload: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"An error occurred while uploading the file: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    import argparse

    port = int(os.getenv('BACKEND_PORT', 8000))

    reload = os.environ.get("ENVIRONMENT", "development") == "development"

    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=reload)
