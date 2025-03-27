from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
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
