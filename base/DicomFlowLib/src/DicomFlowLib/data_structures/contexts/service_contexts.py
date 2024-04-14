import json
import uuid
from io import StringIO
from typing import Any, Dict

import pandas as pd
from pydantic import BaseModel, field_serializer, ConfigDict

from DicomFlowLib.data_structures.flow import Flow, Destination
import pydicom


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
    file_uuid: str


class FlowContext(SCPContext):
    flow: Flow
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

