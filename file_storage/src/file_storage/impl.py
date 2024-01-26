import os
import uuid
from typing import Dict

import uvicorn
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import FileResponse

from DicomFlowLib.log import CollectiveLogger


class FileStorage(FastAPI):
    def __init__(self, logger: CollectiveLogger,
                 base_dir: str,
                 file_storage_host: str,
                 file_storage_port: int,
                 suffix: str = ".tar"):
        super().__init__()

        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)
        self.logger = logger
        self.suffix = suffix
        self.file_storage_host = file_storage_host
        self.file_storage_port = file_storage_port

        @self.post("/")
        def post(tar_file: UploadFile = File(...)):
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
            new_uid = str(uuid.uuid4())
            self.logger.debug(f"Clone file on uid: {uid} to new uid: {new_uid}", finished=False)

            assert self.file_exists(uid)

            os.link(self.get_file_path(uid), self.get_file_path(new_uid))
            self.logger.debug(f"Clone file on uid: {uid} to new uid: {new_uid}", finished=False)
            return uid

        @self.get("/")
        def get(uid: str) -> FileResponse:
            self.logger.debug(f"Serving file with uid: {uid}", finished=False)
            assert self.file_exists(uid)

            self.logger.debug(f"Serving file with uid: {uid}", finished=True)
            return FileResponse(self.get_file_path(uid))

        @self.delete("/")
        def delete(uid: str):
            self.logger.debug(f"Deleting file with uid: {uid}", finished=False)
            assert self.file_exists(uid)
            os.remove(self.get_file_path(uid))
            self.logger.debug(f"Deleting file with uid: {uid}", finished=True)
            return "success"

    def get_file_path(self, uid):
        return os.path.join(self.base_dir, uid + self.suffix)

    def file_exists(self, uid):
        b = os.path.isfile(self.get_file_path(uid))
        if not b:
            raise HTTPException(404, "FileNotFoundError")
        return b

    def start(self):
        uvicorn.run(app=self, host=self.file_storage_host, port=self.file_storage_port)
