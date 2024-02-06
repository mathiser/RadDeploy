from io import StringIO
from typing import Dict, Any

import pandas as pd

from DicomFlowLib.data_structures.flow import Flow, Destination
from .base_context import BaseContext


class FlowContext(BaseContext):
    flow: Flow
    src_uid: str
    dataframe_json: str
    sender: Destination
    mount_mapping: Dict[str, str] = {}
    active_model_idx: int | None = None

    def __init__(self, **data: Any):
        super().__init__(**data)
        self.mount_mapping["src"] = self.src_uid

    @property
    def active_model(self):
        if self.active_model_idx is not None:
            return self.flow.models[self.active_model_idx]
        else:
            raise Exception("Active model not set")
    @property
    def dataframe(self):
        return pd.read_json(StringIO(self.dataframe_json))
