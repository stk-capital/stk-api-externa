from pydantic import BaseModel, Field
from typing import List
from datetime import datetime
import uuid



def uuid_str():
    return str(uuid.uuid4())


class Cluster(BaseModel):
    id: str = Field(default_factory=uuid_str, alias="_id")
    posts_ids: List[str]
    summary: str = Field(default="")
    theme: str = Field(default="")
    relevance_score: float = Field(default=0.0)
    label: int = Field(default=-1)
    was_processed: bool = Field(default=False)
    created_at: datetime = Field(default_factory=datetime.now)

    def add_post(self, post_id):
        self.posts_ids.append(post_id)

    def get_summary(self):
        return "\n".join([post.summary for post in self.posts])
