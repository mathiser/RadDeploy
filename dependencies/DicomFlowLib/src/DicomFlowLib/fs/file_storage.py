import logging
import os
import uuid
from io import BytesIO
from typing import Dict

from DicomFlowLib.log.logger import CollectiveLogger


class FileStorage:
    def __init__(self,
                 base_dir,
                 logger: CollectiveLogger):
        self.base_dir = base_dir

        self.logger = logger

        os.makedirs(self.base_dir, exist_ok=True)
        self.files: Dict[str, str] = {}

    def reload_files(self):
        self.logger.debug("Reloading files", finished=False)
        self.files = {}
        self.files = {f: os.path.join(self.base_dir, f) for f in os.listdir(self.base_dir)}
        self.logger.debug("Reloading files", finished=True)

    def put(self, file: BytesIO) -> str:
        uid = str(uuid.uuid4())
        self.logger.debug(f"Putting file on uid: {uid}", finished=False)

        p = os.path.join(self.base_dir, uid)
        with open(p, "wb") as writer:
            self.logger.debug(f"Writing file with uid: {uid} to path: {p}", finished=False)
            writer.write(file.read())
            self.logger.debug(f"Writing file with uid: {uid} to path: {p}", finished=True)

        self.logger.debug(f"Putting file on uid: {uid}", finished=True)
        return uid

    def get(self, uid):
        self.logger.debug(f"Serving file with uid: {uid}" , finished=False)

        self.reload_files()
        p = os.path.join(self.base_dir, uid)
        if not os.path.exists(p):
            self.logger.error(f"File with uid: {uid} not found", finished=True)
        else:
            self.logger.debug(f"Serving file with uid: {uid}", finished=True)
            return open(p, "rb")

    def delete(self, uid):
        self.logger.debug(f"Deleting file with uid: {uid}", finished=False)

        self.reload_files()
        p = os.path.join(self.base_dir, uid)
        if not os.path.exists(p):
            self.logger.error(f"File with uid: {uid} not found", finished=True)
        else:
            os.unlink(p)
            self.logger.debug(f"Deleting file with uid: {uid}", finished=True)