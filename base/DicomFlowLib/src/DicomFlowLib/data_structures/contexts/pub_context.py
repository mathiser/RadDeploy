from typing import Any

from pydantic import BaseModel

class Base(BaseModel):
    exchange: str
    exchange_type: str = "topic"

class PubModel(Base):
    routing_key_success: str = "success"
    routing_key_fail: str = "fail"

class SubModel(Base):
    routing_key: str
    routing_key_fetch_echo: str | None = None


class PublishContext(PubModel):
    body: bytes
    routing_key: str | None = None
    reply_to: str | None = None
    priority: int = 0