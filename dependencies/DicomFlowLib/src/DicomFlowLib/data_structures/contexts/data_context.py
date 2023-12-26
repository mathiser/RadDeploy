from typing import List, Dict

from pydantic import BaseModel

from DicomFlowLib.data_structures.flow import Flow


class FlowContext(BaseModel):
    file_metas: List = []
    flow: Flow | None = None
    input_file_uid: str | None = None
    output_file_uid: str | None = None

    def add_meta(self, meta: Dict):
        if "7FE00010" in meta.keys():
            del meta["7FE00010"]

        self.file_metas.append(meta)
