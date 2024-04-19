import copy
import logging
import uuid
from io import BytesIO
from DicomFlowLib.fs.client.interface import FileStorageClientInterface
from DicomFlowLib.fs.utils import hash_file


class MockFileStorageClient(FileStorageClientInterface):
    def __init__(self, *args, **kwargs):
        self.tar_files = {}

    def post(self, file: BytesIO) -> str:
        uid = str(uuid.uuid4())
        self.tar_files[uid] = copy.deepcopy(file)
        self.tar_files[uid].seek(0)
        return uid

    def clone(self, uid):
        self.tar_files[uid].seek(0)

        new_uid = str(uuid.uuid4())
        self.tar_files[new_uid] = self.tar_files[uid]
        return new_uid

    def get_hash(self, uid: str):
        self.tar_files[uid].seek(0)

        return hash_file(self.get(uid))

    def exists(self, uid: str) -> bool:
        return uid in self.tar_files.keys()

    def get(self, uid: str):
        self.tar_files[uid].seek(0)

        return self.tar_files[uid]

    def delete(self, uid: str):
        del self.tar_files[uid]
