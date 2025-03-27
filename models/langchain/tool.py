from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, Dict, Union, List
from bson import ObjectId


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

