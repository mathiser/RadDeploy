from pydantic import BaseModel


class Destination(BaseModel):
    host: str
    port: int
    ae_title: str
