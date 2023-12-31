import uuid
from typing import List, Dict, Any

from pydantic import BaseModel

from DicomFlowLib.data_structures.flow import Flow


class FlowContext(BaseModel):
    uid: str | None = None
    file_metas: List = []
    flow: Flow | None = None
    input_file_uid: str | None = None
    output_file_uid: str | None = None

    def __init__(self, **data: Any):
        super().__init__(**data)
        self.uid = str(uuid.uuid4())

    def add_meta(self, meta: Dict):
        if "7FE00010" in meta.keys():
            del meta["7FE00010"]

        self.file_metas.append(meta)
