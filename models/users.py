from pydantic import BaseModel, Field
from datetime import datetime

class User(BaseModel):
    companyId: str  # Reference to Companies collection
    name: str
    handle: str
    avatar: str = "/placeholder.svg?height=400&width=400"
    description: str
    website: str
    followers: str = "0"
    created_at: datetime = Field(default_factory=datetime.now)

    class Config:
        populate_by_name = True
        json_encoders = {
            datetime: lambda dt: dt.isoformat()
        }
