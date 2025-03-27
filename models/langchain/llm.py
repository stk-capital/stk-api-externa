from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from bson import ObjectId

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
