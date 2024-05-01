import uuid
from io import StringIO
from typing import Any, Dict

import pandas as pd
from pydantic import BaseModel, field_serializer, ConfigDict

from RadDeployLib.data_structures.flow import Destination, Flow, Model


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
    model_config = ConfigDict(arbitrary_types_allowed=True)
    flow: Flow
    dataframe: pd.DataFrame

    def __init__(self, dataframe: str | pd.DataFrame, **data: Any):
        if isinstance(dataframe, str):
            dataframe = self.deserialize_dataframe(dataframe)
        super().__init__(dataframe=dataframe, **data)

    def deserialize_dataframe(self, dataframe: str):
        return pd.read_json(StringIO(dataframe))

    @field_serializer('dataframe', when_used='json')
    def serialize_dataframe(self, dataframe: pd.DataFrame) -> str:
        return dataframe.to_json()


class FinishedFlowContext(FlowContext):
    """
    Published by scheduler when a flow is finished
    """
    mount_mapping: Dict[str, str]


class SCUContext(BaseContext):
    """
    Published from the SCU when posted to destination
    """
    finished_flow_context: FinishedFlowContext
    destination: Destination
    status: bool


class JobContext(BaseContext):
    """
    Used for communication between scheduler and consumers
    """
    correlation_id: int
    model: Model
    input_mount_mapping: Dict[str, str] = {}
    output_mount_mapping: Dict[str, str] = {}
    job_log: str = ""


if __name__ == "__main__":
    fc = FlowContext(src_uid="asdf",
                     destination=Destination(host="local", port=123, ae_title="asdf"),
                     flow=Flow(uid="asdf"),
                     dataframe=pd.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]}))
