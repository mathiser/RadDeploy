from pydantic import BaseModel


class PublishContext(BaseModel):
    body: bytes
    routing_key: str
    exchange: str
    exchange_type: str
    routing_key_as_queue: bool
    reply_to: str | None = None
    priority: int = 0