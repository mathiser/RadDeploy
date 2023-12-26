import logging
import os
import uuid
from io import BytesIO
from typing import Dict

from DicomFlowLib.default_config import LOG_FORMAT


class FileStorage:
    def __init__(self,
                 base_dir,
                 log_level: int = 10):
        self.base_dir = base_dir

        logging.basicConfig(level=log_level, format=LOG_FORMAT)
        self.LOGGER = logging.getLogger(__name__)
        os.makedirs(self.base_dir, exist_ok=True)
        self.files: Dict[str, str] = {}

    def reload_files(self):
        self.files = {}
        self.files = {f: os.path.join(self.base_dir, f) for f in os.listdir(self.base_dir)}

    def put(self, file: BytesIO) -> str:
        uid = str(uuid.uuid4())
        with open(os.path.join(self.base_dir, uid), "wb") as writer:
            writer.write(file.read())
        return uid

    def get(self, uid):
        self.reload_files()
        p = os.path.join(self.base_dir, uid)
        if not os.path.exists(p):
            self.LOGGER.error(f"File not found with uid: {uid}")
        else:
            return open(p, "rb")

    def delete(self, uid):
        self.reload_files()
        p = os.path.join(self.base_dir, uid)
        if not os.path.exists(p):
            self.LOGGER.error(f"File not found with uid: {uid}")
        else:
            os.unlink(p)