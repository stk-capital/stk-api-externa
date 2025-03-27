
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
import uuid
def uuid_str():
    return str(uuid.uuid4())


class Chunk(BaseModel):
    id: str = Field(default_factory=uuid_str, alias="_id")
    content: str
    summary: str
    subject: Optional[str] = None
    source: str
    instrument_ids: Optional[List[str]] = None
    embedding: List[float]
    include: bool
    has_events: bool
    document_id: str
    document_collection: str
    index: int  # Index of the chunk in the document
    published_at: datetime = Field(default_factory=datetime.now)
    created_at: datetime = Field(default_factory=datetime.now)
    was_processed: bool = False  # Flag for processing status
    was_processed_events: bool = False  # Flag for event processing status

    @property
    def email_id(self) -> str:
        if self.document_collection != "emails":
            raise ValueError(f'source_collection is not "emails": {self.document_collection}')
        return self.document_id

    @email_id.setter
    def email_id(self, value: str):
        self.document_id = value
        self.document_collection = "emails"
