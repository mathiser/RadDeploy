
from pydantic import BaseModel


class PublishContext(BaseModel):
    body: bytes
    routing_key: str | None = None
    reply_to: str | None = None
    priority: int = 0
