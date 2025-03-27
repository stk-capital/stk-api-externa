from pydantic import BaseModel, Field
from typing import List
from datetime import datetime
import uuid
def uuid_str():
    return str(uuid.uuid4())

class Info(BaseModel):
    id: str = Field(default_factory=uuid_str, alias="_id")
    embedding: List[float]
    chunk_ids: List[str]
    created_at: datetime = Field(default_factory=datetime.now)
    last_updated: datetime = Field(default_factory=datetime.now)
    # Fields for associating companies and sources
    companiesId: List[str] = Field(default_factory=list)
    sourcesId: List[str] = Field(default_factory=list)
