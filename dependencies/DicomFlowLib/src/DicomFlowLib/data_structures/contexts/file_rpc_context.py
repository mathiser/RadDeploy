from pydantic import BaseModel


class FileStorageRPCContext(BaseModel):
    id: str
    method: str  # GET POST DELETE
    queue: str