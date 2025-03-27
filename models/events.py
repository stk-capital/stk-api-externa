
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from uuid import uuid4

def uuid_str():
    return str(uuid4())


class Event(BaseModel):
    id: str = Field(default_factory=uuid_str, alias="_id")
    name: str  # The name of the event
    description: str  # Brief description of the event
    date: Optional[datetime] = None  # Normalized date in UTC
    original_date_text: str  # Original text describing the date
    location: Optional[str] = None  # Physical or virtual location
    event_type: str  # Category: earnings_call, investor_conference, etc.
    companies_ids: List[str] = Field(default_factory=list)  # Companies involved
    chunk_ids: List[str] = Field(default_factory=list)  # Source chunks
    source: str  # Source of information
    confirmed: bool = True  # Is this a confirmed event or speculative
    confidence: float = 1.0  # Confidence score (0.0-1.0)
    embedding: List[float]  # For similarity search
    created_at: datetime = Field(default_factory=datetime.now)
    last_updated: datetime = Field(default_factory=datetime.now)
