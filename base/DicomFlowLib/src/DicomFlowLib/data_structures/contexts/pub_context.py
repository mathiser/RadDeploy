
from pydantic import BaseModel


class PublishContext(BaseModel):
    body: bytes
    pub_model_routing_key: str
    reply_to: str | None = None
    priority: int = 0
