from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
import uuid
def uuid_str():
    return str(uuid.uuid4())

class Companies(BaseModel):
    id: str = Field(default_factory=uuid_str, alias="_id")
    name: str
    ticker: str
    public: bool
    parent_company: str
    description: str  # Existing field
    sector: Optional[str] = None  # New sector field
    embedding: List[float]
    created_at: datetime = Field(default_factory=datetime.now)
