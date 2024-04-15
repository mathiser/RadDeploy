from typing import List, Dict

from pydantic import BaseModel


class PubModel(BaseModel):
    exchange: str | None = None
    exchange_type: str = "topic"
    routing_key_values: Dict[str, str] = {
        "SUCCESS": "success",  # When something runs as intended
        "FAIL": "fail",  # When something fails, this is used intended in Results
        "ERROR": "error",  # When an uncaught exception occurs during a mq process
    }

class SubModel(BaseModel):
    exchange: str | None = None
    exchange_type: str = "topic"
    routing_keys: List[str]
    routing_key_fetch_echo: str | None = None
