import os
import shutil
import tempfile
from dataclasses import dataclass, field
from typing import List, Dict, Any

from dataclass_wizard import JSONWizard
import hashlib
import tarfile
from io import BytesIO

from DicomFlowLib.data_structures.contexts import FlowContext


@dataclass
class FileContext:
    # class Config:
    #     arbitrary_types_allowed = True

    file: bytes | str | None = None
    checksum: str | None = None

    def add_file(self, file: BytesIO):
        file.seek(0)
        self.file = file.read()
        self.checksum = hashlib.sha256(self.file).hexdigest()
        file.close()

    def generate_tar_from_path(self, path):
        file = BytesIO()
        with tarfile.TarFile.open(fileobj=file, mode="w:gz") as tf:
            tf.add(path, arcname=path)
        file.seek(0)
        self.add_file(file)


@dataclass
class SCPContext(JSONWizard):
    path: str = field(default_factory=tempfile.mkdtemp)
    fileMetas: List = field(default_factory=list)
    file: FileContext | None = None
    flow: FlowContext | None = None

    def __del__(self):
        if self.path:
            if os.path.isdir(self.path):
                shutil.rmtree(self.path)

    def add_meta(self, meta: Dict):
        if "7FE00010" in meta.keys():
            del meta["7FE00010"]

        self.fileMetas.append(meta)

    def generate_tar_binary(self):
        self.file = FileContext()
        self.file.generate_tar_from_path(self.path)
