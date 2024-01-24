from pydantic import BaseModel


class MQEntrypointResult(BaseModel):
    body: bytes
    priority: int = 0