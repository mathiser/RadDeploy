import os
import shutil
import tempfile
from typing import List, Dict, Any

from pydantic import BaseModel


class SCPContext(BaseModel):
    path: str | None = None
    file_metas: List = []

    file_queue: str | None = None
    file_exchange: str | None = None
    file_checksum: str | None = None

    def __init__(self, **data: Any):
        super().__init__(**data)
        self.path = tempfile.mkdtemp()

    def __del__(self):
        if self.path:
            if os.path.isdir(self.path):
                shutil.rmtree(self.path)

    def add_meta(self, meta: Dict):
        if "7FE00010" in meta.keys():
            del meta["7FE00010"]

        self.file_metas.append(meta)
