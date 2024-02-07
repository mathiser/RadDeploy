import logging
import os
import uuid

import uvicorn
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import FileResponse

from DicomFlowLib.fs.utils import hash_file


class FileStorageServer(FastAPI):
    def __init__(self,
                 base_dir: str,
                 host: str,
                 port: int,
                 suffix: str = ".tar",
                 allow_post: bool = True,
                 allow_get: bool = True,
                 allow_clone: bool = True,
                 allow_delete: bool = True,
                 delete_on_get: bool = False,
                 log_level: int = 20):
        super().__init__()

        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        self.suffix = suffix
        self.host = host
        self.port = port
        self.allow_post = allow_post
        self.allow_get = allow_get
        self.allow_delete = allow_delete
        self.allow_clone = allow_clone
        self.delete_on_get = delete_on_get

        @self.post("/")
        def post(tar_file: UploadFile = File(...)):
            if not self.allow_post:
                raise HTTPException(status_code=405, detail="Method not allowed")
            return self.post_file(tar_file)

        @self.put("/")
        def clone(uid: str):
            if not self.allow_clone:
                raise HTTPException(status_code=405, detail="Method not allowed")
            if not self.file_exists(uid):
                raise HTTPException(404, "FileNotFoundError")
            return self.clone_file(uid)

        @self.delete("/")
        def delete(uid: str):
            if not self.allow_delete:
                raise HTTPException(status_code=405, detail="Method not allowed")
            if not self.file_exists(uid):
                raise HTTPException(404, "FileNotFoundError")
            return self.delete_file(uid)

        @self.get("/")
        def get(uid: str) -> FileResponse:
            if not self.allow_get:
                raise HTTPException(status_code=405, detail="Method not allowed")
            if not self.file_exists(uid):
                raise HTTPException(404, "FileNotFoundError")
            return FileResponse(self.get_file(uid))

        @self.get("/hash")
        def get_hash(uid: str):
            if not self.allow_get:
                raise HTTPException(status_code=405, detail="Method not allowed")
            if not self.file_exists(uid):
                raise HTTPException(404, "FileNotFoundError")
            return self.get_hash(uid)
        
        @self.get("/exists")
        def exists(uid: str):
            if not self.allow_get:
                raise HTTPException(status_code=405, detail="Method not allowed")
            return self.file_exists(uid)

    def get_hash(self, uid: str):
        self.logger.debug(f"Serving hash of: {uid}")
        return hash_file(self.get_file_path(uid))

    def get_file(self, uid: str):
        self.logger.debug(f"Serving file with uid: {uid}")
        try:
            return self.get_file_path(uid)
        finally:
            if self.delete_on_get:
                self.delete_file(uid)

    def delete_file(self, uid: str):
        self.logger.debug(f"Deleting file with uid: {uid}")
        os.remove(self.get_file_path(uid))
        return "success"

    def clone_file(self, uid: str):
        new_uid = str(uuid.uuid4())
        self.logger.debug(f"Clone file on uid: {uid} to new uid: {new_uid}")
        os.link(self.get_file_path(uid), self.get_file_path(new_uid))
        return new_uid

    def post_file(self, tar_file):
        uid = str(uuid.uuid4())
        self.logger.debug(f"Putting file on uid: {uid}")

        p = self.get_file_path(uid)
        with open(p, "wb") as writer:
            self.logger.debug(f"Writing file with uid: {uid} to path: {p}")
            writer.write(tar_file.file.read())

        tar_file.file.close()
        return uid

    def get_file_path(self, uid):
        return os.path.join(self.base_dir, uid + self.suffix)
    def get_uid_from_path(self, path: str):
        return path.replace(self.suffix, "").replace(self.base_dir, "").strip("/")

    def file_exists(self, uid):
        return os.path.isfile(self.get_file_path(uid))
       
    def start(self):
        uvicorn.run(app=self, host=self.host, port=self.port)
