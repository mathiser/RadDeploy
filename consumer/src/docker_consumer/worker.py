import enum
from typing import Any

from pydantic import BaseModel


class WorkerType(enum.Enum):
    CPU = "CPU"
    GPU = "GPU"


class Worker(BaseModel):
    type: WorkerType
    device_id: str

    def __init__(self, **data: Any):
        super().__init__(**data)

    def is_gpu_worker(self):
        print(self.type)
        print(type(self.type))
        print(self.type == WorkerType.GPU)
        return self.type == WorkerType.GPU
