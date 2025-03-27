from pydantic import BaseModel, Field
from datetime import datetime
from typing import List

class Post(BaseModel):
    infoId: str  # Reference to Info collection
    userId: str  # Reference to User collection
    source: str
    title: str
    content: str
    timestamp: str
    avatar: str = "/placeholder.svg?height=40&width=40"
    likes: int = 0
    dislikes: int = 0
    shares: int = 0
    embedding: List[float]
    created_at: datetime = Field(default_factory=datetime.now)

    class Config:
        populate_by_name = True
        json_encoders = {
            datetime: lambda dt: dt.isoformat()
        }