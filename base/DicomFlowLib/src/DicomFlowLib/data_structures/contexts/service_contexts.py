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
    model_config = ConfigDict(arbitrary_types_allowed=True)

    sender: Destination | None = None
    src_uid: str | None = None
    dataframe: pd.DataFrame | None = None

    def __init__(self, dataframe: str | pd.DataFrame | None = None, **data: Any):
        if isinstance(dataframe, str):
            dataframe = self.deserialize_dataframe(dataframe)
        super().__init__(dataframe=dataframe, **data)

    def add_meta_row(self, dcm_path: str, ds: pydicom.dataset.Dataset):
        elems = {"dcm_path": dcm_path}

        for elem in ds:
            if elem.keyword == "PixelData":
                continue
            else:
                elems[str(elem.keyword)] = str(elem.value)

        self.dataframe = pd.concat([self.dataframe, pd.DataFrame([elems])], ignore_index=True)

    def deserialize_dataframe(self, dataframe: str):
        return pd.read_json(StringIO(dataframe))

    @field_serializer('dataframe', when_used='json')
    def serialize_dataframe(self, dataframe: pd.DataFrame) -> str:
        return dataframe.to_json()


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


if __name__ == "__main__":
    m = SCPContext()
    m.dataframe = pd.DataFrame()
    m.add_meta_row("/1.dcm", {"A": 1, "B": 2})
    m.add_meta_row("/2.dcm", {"A": 2, "B": 3})
    print(m.dataframe)
    j = m.model_dump_json()
    new_m = SCPContext(**json.loads(j))
    print(new_m.dataframe)
