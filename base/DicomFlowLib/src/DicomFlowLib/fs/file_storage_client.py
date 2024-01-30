import os
from io import BytesIO

import requests
import urllib.parse

from DicomFlowLib.fs.utils import hash_file
from DicomFlowLib.log import CollectiveLogger


class FileStorageClient:
    def __init__(self, logger: CollectiveLogger,
                 file_storage_url: str,
                 local_cache: str | None = None,
                 suffix: str = ".tar"):
        super().__init__()
        self.suffix = suffix
        self.logger = logger
        self.url = file_storage_url
        self.local_cache = local_cache
        if self.local_cache:
            os.makedirs(self.local_cache, exist_ok=True)

    def post(self, file: BytesIO) -> str:
        res = requests.post(self.url, files={"tar_file": file})
        if res.ok:
            uid = res.json()
            if self.local_cache:
                self.write_file_to_disk(uid, file)
            return uid
        elif res.status_code == 404:
            raise FileNotFoundError
        else:
            self.logger.error(res.json())
            res.raise_for_status()

    def clone(self, uid):
        self.logger.debug(f"Clone file on uid: {uid}", finished=False)
        res = requests.put(self.url, params={"uid": uid})
        if res.ok:
            self.logger.debug(f"Clone file on uid: {uid}", finished=True)
            new_uid = res.json()
            if self.local_cache:
                os.link(self.get_file_path(uid), self.get_file_path(new_uid))
            return new_uid
        elif res.status_code == 404:
            raise FileNotFoundError
        else:
            self.logger.error(res.json())
            res.raise_for_status()

    def get_hash(self, uid: str):
        res = requests.get(urllib.parse.urljoin(self.url, "hash"), params={"uid": uid})
        if res.ok:
            self.logger.debug(f"Serving hash of file: {uid}", finished=True)
            return res.json()
        elif res.status_code == 404:
            raise FileNotFoundError
        else:
            self.logger.error(res.json())

    def get(self, uid: str):
        self.logger.debug(f"Serving file with uid: {uid}", finished=False)

        if self.local_cache:
            if self.file_exists(uid):
                if self.remote_local_match(uid):
                    return open(self.get_file_path(uid), "rb")

        res = requests.get(self.url, params={"uid": uid})
        if res.ok:
            self.logger.debug(f"Serving file with uid: {uid}", finished=True)
            file = BytesIO(res.content)
            if self.local_cache:
                self.write_file_to_disk(uid, file)
            return file
        elif res.status_code == 404:
            raise FileNotFoundError
        else:
            self.logger.error(res.json())
            res.raise_for_status()

    def delete(self, uid: str):
        self.logger.debug(f"Deleting file with uid: {uid}", finished=False)
        res = requests.delete(self.url, params={"uid": uid})

        if self.local_cache:
            if self.file_exists(uid):
                os.remove(self.get_file_path(uid))

        if res.ok:
            self.logger.debug(f"Deleting file with uid: {uid}", finished=True)
            return res.json()
        elif res.status_code == 404:
            raise FileNotFoundError
        else:
            self.logger.error(res.json())
            res.raise_for_status()

    def get_file_path(self, uid):
        if self.local_cache:
            return os.path.join(self.local_cache, uid + self.suffix)
        else:
            raise Exception("Local cache not set")

    def file_exists(self, uid):
        b = os.path.isfile(self.get_file_path(uid))
        return b

    def remote_local_match(self, uid):
        self.logger.debug(f"Checking for a newer file remote", finished=False)
        remote_hash = self.get_hash(uid)
        self.logger.debug(f"Remote hash: {remote_hash}", finished=True)
        local_hash = hash_file(self.get_file_path(uid))
        self.logger.debug(f"local hash: {local_hash}", finished=True)

        return remote_hash == local_hash

    def write_file_to_disk(self, uid: str, file: BytesIO):
        p = self.get_file_path(uid)
        file.seek(0)
        with open(p, "wb") as writer:
            self.logger.debug(f"Writing file with uid: {uid} to path: {p}", finished=False)
            writer.write(file.read())
            self.logger.debug(f"Writing file with uid: {uid} to path: {p}", finished=True)
        file.seek(0)
        return p
