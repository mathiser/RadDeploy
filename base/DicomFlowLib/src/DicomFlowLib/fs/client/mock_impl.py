import logging
import uuid
from io import BytesIO
from .interface import FileStorageClientInterface
from DicomFlowLib.fs.utils import hash_file


class MockFileStorageClient(FileStorageClientInterface):
    def __init__(self):
        self.tar_files = {}

    def post(self, file: BytesIO) -> str:
        uid = str(uuid.uuid4())
        self.tar_files[uid] = file
        return uid

    def clone(self, uid):
        new_uid = str(uuid.uuid4())
        self.tar_files[new_uid] = self.tar_files[uid]
        return new_uid

    def get_hash(self, uid: str):
        return hash_file(self.get(uid))

    def exists(self, uid: str) -> bool:
        return uid in self.tar_files.keys()

    def get(self, uid: str):
        return self.tar_files[uid]

    def delete(self, uid: str):
        del self.tar_files[uid]
