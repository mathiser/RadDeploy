import os
import shutil
import uuid
from io import BytesIO

from DicomFlowLib.log.logger import CollectiveLogger


class FileStorage:
    def __init__(self,
                 logger: CollectiveLogger,
                 base_dir: str):
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)
        self.logger = logger


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

        p = os.path.join(self.base_dir, uid)
        if not os.path.exists(p):
            self.logger.error(f"File with uid: {uid} not found", finished=True)
        else:
            self.logger.debug(f"Serving file with uid: {uid}", finished=True)
            return open(p, "rb")

    def delete(self, uid):
        self.logger.debug(f"Deleting file with uid: {uid}", finished=False)

        p = os.path.join(self.base_dir, uid)
        if not os.path.exists(p):
            self.logger.error(f"File with uid: {uid} not found", finished=True)
        else:
            shutil.rmtree(p)
            self.logger.debug(f"Deleting file with uid: {uid}", finished=True)