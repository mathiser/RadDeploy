import uuid
from typing import List, Dict, Any

from pydantic import BaseModel

from DicomFlowLib.data_structures.flow import Flow, Destination


class FlowContext(BaseModel):
    sender: Destination | None = None
    uid: str | None = None
    file_metas: List = []

    flow: Flow | None = None
    flow_instance_uid: str | None = None

    input_file_uid: str | None = None
    output_file_uid: str | None = None

    def __init__(self, **data: Any):
        super().__init__(**data)
        if not self.uid:
            self.uid = self.generate_uid()
    def add_flow(self, flow: Flow):
        self.flow = flow
        self.flow_instance_uid = self.generate_uid()

    def add_meta(self, meta: Dict):
        if "7FE00010" in meta.keys():  ## This is the binary_data tag.
            del meta["7FE00010"]

        self.file_metas.append(meta)

    @staticmethod
    def generate_uid():
        return str(uuid.uuid4())
