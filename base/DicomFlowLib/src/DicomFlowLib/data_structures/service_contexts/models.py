import uuid
from typing import Any, Dict

from pydantic import BaseModel

from DicomFlowLib.data_structures.flow import Destination, Flow, Model


def generate_uid():
    return str(uuid.uuid4())


class BaseContext(BaseModel):
    uid: str | None = None

    def __init__(self, **data: Any):
        super().__init__(**data)
        if not self.uid:
            self.uid = generate_uid()


class SCPContext(BaseContext):
    sender: Destination
    src_uid: str


class FlowContext(SCPContext):
    flow: Flow


class ModelContext(BaseContext):
    correlation_id: str | None = None
    model: Model
    mount_mapping: Dict[str, str]

    def __init__(self, **data: Any):
        super().__init__(**data)
        if not self.uid:
            self.uid = generate_uid()

class FlowFinishedContext(FlowContext):
    dst_uid: str
