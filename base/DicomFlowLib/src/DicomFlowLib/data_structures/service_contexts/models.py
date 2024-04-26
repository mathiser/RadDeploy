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
    """
    Being published by the storescp when DICOM files are received.
    """
    sender: Destination
    src_uid: str


class FlowContext(SCPContext):
    """
    Published by the fingerprinter when a flow is matched to received DICOM tar
    """
    flow: Flow


class FinishedFlowContext(FlowContext):
    """
    Published by scheduler when a flow is finished
    """
    mount_mapping: Dict[str, str]



class JobContext(BaseContext):
    """
    Used for communication between scheduler and consumers
    """
    correlation_id: int
    model: Model
    input_mount_mapping: Dict[str, str] = {}
    output_mount_mapping: Dict[str, str] = {}
