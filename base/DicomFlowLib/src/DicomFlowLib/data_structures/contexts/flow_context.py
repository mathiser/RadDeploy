from typing import Dict, Any
from .base_context import BaseContext
from DicomFlowLib.data_structures.flow import Flow


class FlowContext(BaseContext):
    flow: Flow
    src_uid: str
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