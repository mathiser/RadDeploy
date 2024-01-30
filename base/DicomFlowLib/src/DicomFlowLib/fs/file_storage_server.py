import hashlib
import os
import uuid

import uvicorn
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import FileResponse

from DicomFlowLib.fs.utils import hash_file
from DicomFlowLib.log import CollectiveLogger


class FileStorageServer(FastAPI):
    def __init__(self, logger: CollectiveLogger,
                 base_dir: str,
                 host: str,
                 port: int,
                 suffix: str = ".tar",
                 allow_post: bool = True,
                 allow_get: bool = True,
                 allow_clone: bool = True,
                 allow_delete: bool = True):
        super().__init__()

        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)
        self.logger = logger
        self.suffix = suffix
        self.host = host
        self.port = port
        self.allow_post = allow_post
        self.allow_get = allow_get
        self.allow_delete = allow_delete
        self.allow_clone = allow_clone

        @self.post("/")
        def post(tar_file: UploadFile = File(...)):
            if not self.allow_post:
                raise HTTPException(status_code=405, detail="Method not allowed")
            uid = str(uuid.uuid4())
            self.logger.debug(f"Putting file on uid: {uid}", finished=False)

            p = self.get_file_path(uid)
            with open(p, "wb") as writer:
                self.logger.debug(f"Writing file with uid: {uid} to path: {p}", finished=False)
                writer.write(tar_file.file.read())
                self.logger.debug(f"Writing file with uid: {uid} to path: {p}", finished=True)

            self.logger.debug(f"Putting file on uid: {uid}", finished=True)
            tar_file.file.close()
            return uid

        @self.put("/")
        def clone(uid: str):
            if not self.allow_clone:
                raise HTTPException(status_code=405, detail="Method not allowed")
            new_uid = str(uuid.uuid4())
            self.logger.debug(f"Clone file on uid: {uid} to new uid: {new_uid}", finished=False)

            assert self.file_exists(uid)

            os.link(self.get_file_path(uid), self.get_file_path(new_uid))
            self.logger.debug(f"Clone file on uid: {uid} to new uid: {new_uid}", finished=False)
            return uid

        @self.get("/")
        def get(uid: str) -> FileResponse:
            if not self.allow_get:
                raise HTTPException(status_code=405, detail="Method not allowed")

            self.logger.debug(f"Serving file with uid: {uid}", finished=False)

            assert self.file_exists(uid)

            self.logger.debug(f"Serving file with uid: {uid}", finished=True)
            return FileResponse(self.get_file_path(uid))

        @self.delete("/")
        def delete(uid: str):
            if not self.allow_delete:
                raise HTTPException(status_code=405, detail="Method not allowed")
            self.logger.debug(f"Deleting file with uid: {uid}", finished=False)
            assert self.file_exists(uid)
            os.remove(self.get_file_path(uid))
            self.logger.debug(f"Deleting file with uid: {uid}", finished=True)
            return "success"

        @self.get("/")
        def get(uid: str) -> FileResponse:
            if not self.allow_get:
                raise HTTPException(status_code=405, detail="Method not allowed")

            self.logger.debug(f"Serving file with uid: {uid}", finished=False)

            assert self.file_exists(uid)

            self.logger.debug(f"Serving file with uid: {uid}", finished=True)
            return FileResponse(self.get_file_path(uid))

        @self.get("/hash")
        def get_hash(uid: str) -> str:
            if not self.allow_get:
                raise HTTPException(status_code=405, detail="Method not allowed")

            self.logger.debug(f"Serving hash of: {uid}", finished=False)

            assert self.file_exists(uid)

            self.logger.debug(f"Serving hash of: {uid}", finished=True)
            return hash_file(self.get_file_path(uid))

    def get_file_path(self, uid):
        return os.path.join(self.base_dir, uid + self.suffix)

    def file_exists(self, uid):
        b = os.path.isfile(self.get_file_path(uid))
        if not b:
            raise HTTPException(404, "FileNotFoundError")
        return b

    def start(self):
        uvicorn.run(app=self, host=self.host, port=self.port)
