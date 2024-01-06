from pydantic import BaseModel


class PubSubQueue(BaseModel):
    routing_key: str
    exchange: str = ""
    exchange_type: str = "direct"
    routing_key_as_queue: str = "SYSTEM"
    x_max_priority: int = 5
