from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from bson import ObjectId


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



class Node(BaseModel):
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    type: str  # 'agent' or 'tool'
    reference_id: str  # ID of the agent or tool
    position: Dict[str, int]  # {x: int, y: int}

