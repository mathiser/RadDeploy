import hashlib
import logging
import os
import threading
import uuid
from io import BytesIO

from file_storage.src.file_manager.delete_daemon import DeleteDaemon


def hash_file(path, buffer_size=65536):
    # BUF_SIZE is totally arbitrary, change for your app!
    sha1 = hashlib.sha1()

    with open(path, 'rb') as f:
        while True:
            data = f.read(buffer_size)
            if not data:
                break
            sha1.update(data)
    return sha1.hexdigest()


class FileManager(threading.Thread):
    def __init__(self,
                 base_dir,
                 suffix: str = ".tar",
                 log_level: int = 20,
                 delete_run_interval: int = 60,
                 delete_files_after: int = -1):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)
        self.suffix = suffix

        self.delete_daemon = DeleteDaemon(base_dir=base_dir,
                                          delete_run_interval=delete_run_interval,
                                          delete_files_after=delete_files_after,
                                          log_level=log_level).start()

    def get_hash(self, uid: str):
        self.logger.debug(f"Serving hash of: {uid}")
        return hash_file(self.get_file_path(uid))

    def get_file(self, uid: str):
        self.logger.debug(f"Serving file with uid: {uid}")
        return self.get_file_path(uid)

    def delete_file(self, uid: str):
        self.logger.debug(f"Deleting file with uid: {uid}")
        os.remove(self.get_file_path(uid))
        return "success"

    def clone_file(self, uid: str):
        new_uid = str(uuid.uuid4())
        self.logger.debug(f"Clone file on uid: {uid} to new uid: {new_uid}")
        os.link(self.get_file_path(uid), self.get_file_path(new_uid))
        return new_uid

    def post_file(self, tar_file: BytesIO):
        uid = str(uuid.uuid4())
        self.logger.debug(f"Putting file on uid: {uid}")

        p = self.get_file_path(uid)
        with open(p, "wb") as writer:
            self.logger.debug(f"Writing file with uid: {uid} to path: {p}")
            writer.write(tar_file.read())

        tar_file.close()
        return uid

    def get_file_path(self, uid):
        return os.path.join(self.base_dir, uid + self.suffix)

    def get_uid_from_path(self, path: str):
        return path.replace(self.suffix, "").replace(self.base_dir, "").strip("/")

    def file_exists(self, uid):
        return os.path.isfile(self.get_file_path(uid))
