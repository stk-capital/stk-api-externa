from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
from bs4 import BeautifulSoup
import uuid     
from typing import Dict, Any
from util.models_utils import custom_encoder


def uuid_str():
    return str(uuid.uuid4())


class Attachment(BaseModel):
    filename: str
    size: int
    type: str
    pdf_metadata: Optional[str] = None



class Email(BaseModel):
    id: str = Field(default_factory=uuid_str, alias="_id")
    message_id: str = Field(default_factory=uuid_str)
    conversation_id: str = Field(default_factory=uuid_str)
    from_address: str
    subject: str
    body: str
    received_at: datetime = Field(default_factory=datetime.now)
    attachments: List[Attachment] = []
    was_processed: bool = False
    relevant: Optional[bool] = None

    @property
    def body_text(self) -> str:
        soup = BeautifulSoup(self.body, "html.parser")
        return soup.get_text(separator=" ", strip=True)

    @property
    def body_pretty(self) -> str:
        soup = BeautifulSoup(self.body, "html.parser")
        return soup.get_text(separator="\n", strip=True)

    def get_lines_pretty(self, numbered: bool = False) -> List[str]:
        lines = self.body_pretty.split("\n")
        if numbered:
            return [f"{i}: {line}" for i, line in enumerate(lines)]
        return lines

    def get_document_pretty(self) -> str:
        lines = self.get_lines_pretty(numbered=True)
        return "\n".join(lines)

    def to_formatted_dict(self, format: str = "html", *args, **kwargs) -> Dict[str, Any]:
        model = custom_encoder(self)
        if format == "html":
            return model
        model.pop("body", None)
        if format == "text":
            model["body_text"] = self.body_text
            return model
        if format == "pretty":
            model["body_pretty"] = self.body_pretty
            return model
        raise ValueError(f"Unknown format {format}")
