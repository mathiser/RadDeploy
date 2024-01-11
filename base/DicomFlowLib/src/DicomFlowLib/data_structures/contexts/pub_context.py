from typing import Any

from pydantic import BaseModel


class PubSubBase(BaseModel):
    routing_key: str
    exchange: str
    exchange_type: str
    routing_key_as_queue: bool


class PublishContext(PubSubBase):
    body: bytes
    reply_to: str | None = None
    priority: int = 0