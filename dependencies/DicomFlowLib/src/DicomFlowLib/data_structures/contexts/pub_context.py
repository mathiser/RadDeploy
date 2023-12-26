from pydantic import BaseModel


class PublishContext(BaseModel):
    body: bytes
    routing_key: str
    exchange: str = ""
    exchange_type: str = "direct"
    routing_key_as_queue: bool = True
    reply_to: str | None = None