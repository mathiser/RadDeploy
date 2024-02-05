from pydantic import BaseModel


class PubModel(BaseModel):
    exchange: str | None = None
    exchange_type: str = "topic"


class SubModel(PubModel):
    routing_key: str
    routing_key_fetch_echo: str | None = None

