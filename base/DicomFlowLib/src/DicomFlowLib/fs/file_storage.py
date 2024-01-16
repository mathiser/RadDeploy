import os
import uuid
from io import BytesIO

from DicomFlowLib.log import CollectiveLogger


class FileStorage:
    def __init__(self,
                 logger: CollectiveLogger,
                 base_dir: str,
                 suffix: str = ".tar"):
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)
        self.logger = logger
        self.suffix = suffix

    def get_file_path(self, uid):
        return os.path.join(self.base_dir, uid + self.suffix)

    def file_exists(self, uid):
        b = os.path.isfile(self.get_file_path(uid))
        if not b:
            self.logger.error(f"File with uid {self.get_file_path(uid)} not found", finished=True)
        return b

    def put(self, file: BytesIO) -> str:
        uid = str(uuid.uuid4())
        self.logger.debug(f"Putting file on uid: {uid}", finished=False)

        p = self.get_file_path(uid)
        with open(p, "wb") as writer:
            self.logger.debug(f"Writing file with uid: {uid} to path: {p}", finished=False)
            writer.write(file.read())
            self.logger.debug(f"Writing file with uid: {uid} to path: {p}", finished=True)

        self.logger.debug(f"Putting file on uid: {uid}", finished=True)
        file.close()
        return uid

    def clone(self, uid):
        new_uid = str(uuid.uuid4())
        self.logger.debug(f"Clone file on uid: {uid} to new uid: {new_uid}", finished=False)
        if not self.file_exists(uid):
            raise FileNotFoundError
        os.link(self.get_file_path(uid), self.get_file_path(new_uid))
        self.logger.debug(f"Clone file on uid: {uid} to new uid: {new_uid}", finished=False)
        return new_uid

    def get(self, uid):
        self.logger.debug(f"Serving file with uid: {uid}", finished=False)
        if not self.file_exists(uid):
            raise FileNotFoundError
        self.logger.debug(f"Serving file with uid: {uid}", finished=True)
        return open(self.get_file_path(uid), "rb")

    def delete(self, uid):
        self.logger.debug(f"Deleting file with uid: {uid}", finished=False)
        if not self.file_exists(uid):
            raise FileNotFoundError
        os.remove(self.get_file_path(uid=uid))
        self.logger.debug(f"Deleting file with uid: {uid}", finished=True)