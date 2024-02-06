from typing import List, Dict

from .base_context import BaseContext
from DicomFlowLib.data_structures.flow import Destination


class SCPContext(BaseContext):
    sender: Destination | None = None
    input_file_uid: str | None = None
    file_metas: List = []

    def add_meta(self, meta: Dict):
        if "7FE00010" in meta.keys():  ## This is the binary_data tag.
            del meta["7FE00010"]

        self.file_metas.append(meta)

