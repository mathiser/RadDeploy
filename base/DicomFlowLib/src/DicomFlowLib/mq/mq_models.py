from typing import List, Dict, Any

from pydantic import BaseModel


class PubModel(BaseModel):
    exchange: str | None = None
    exchange_type: str = "topic"
    routing_key_values: Dict[str, str] = {
        "SUCCESS": "success",  # When something runs as intended
        "FAIL": "fail",  # When something fails, this is used intended in Results
        "ERROR": "error",  # When an uncaught exception occurs during a mq process
    }

    def __init__(self, routing_key_values: Dict | None = None, **data: Any):
        super().__init__(**data)
        if routing_key_values:
            self.routing_key_values = {**self.routing_key_values, **routing_key_values}


class SubModel(BaseModel):
    exchange: str | None = None
    exchange_type: str = "topic"
    routing_keys: List[str]
    routing_key_fetch_echo: str | None = None


if __name__ == "__main__":
    p = PubModel(exchange="test", exchange_type="test", routing_key_values={"HO": "HEI"})
    print(p.routing_key_values)