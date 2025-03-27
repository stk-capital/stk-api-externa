from pydantic import BaseModel, Field
from typing import Optional, Dict, Union, Any
from bson import ObjectId
from datetime import datetime
from pydantic import model_validator


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