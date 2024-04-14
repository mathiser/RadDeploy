from typing import List

from pydantic import BaseModel


class PubModel(BaseModel):
    exchange: str | None = None
    exchange_type: str = "topic"
    routing_key_success: str = "success"
    routing_key_fail: str = "fail"
    routing_key_error: str = "error"


class SubModel(BaseModel):
    exchange: str | None = None
    exchange_type: str = "topic"
    routing_keys: List[str]
    routing_key_fetch_echo: str | None = None
