from typing import List

from pydantic import BaseModel


class PubModel(BaseModel):
    exchange: str | None = None
    exchange_type: str = "topic"


class SubModel(PubModel):
    routing_keys: List[str]
    routing_key_fetch_echo: str | None = None


class PublishContext(BaseModel):
    body: bytes
    routing_key: str | None = None
    reply_to: str | None = None
    priority: int = 0
