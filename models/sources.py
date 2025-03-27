from pydantic import BaseModel, Field
from typing import List
from datetime import datetime
import uuid
def uuid_str():
    return str(uuid.uuid4())

class Source(BaseModel):
    id: str = Field(default_factory=uuid_str, alias="_id")
    name: str
    embedding: List[float]
    created_at: datetime = Field(default_factory=datetime.now)

