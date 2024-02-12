import logging

from fastapi import File, UploadFile, HTTPException, APIRouter
from fastapi.responses import FileResponse

from DicomFlowLib.fs.file_manager import FileManager


class FileStorageRouter(APIRouter):
    def __init__(self,
                 file_manager: FileManager,
                 allow_post: bool = True,
                 allow_get: bool = True,
                 allow_clone: bool = True,
                 allow_delete: bool = True,
                 log_level: int = 20):
        super().__init__()

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)

        self.file_manager = file_manager

        self.allow_post = allow_post
        self.allow_get = allow_get
        self.allow_delete = allow_delete
        self.allow_clone = allow_clone

        @self.post("/")
        def post(tar_file: UploadFile = File(...)):
            if not self.allow_post:
                raise HTTPException(status_code=405, detail="Method not allowed")
            return self.file_manager.post_file(tar_file)

        @self.put("/")
        def clone(uid: str):
            if not self.allow_clone:
                raise HTTPException(status_code=405, detail="Method not allowed")
            if not self.file_manager.file_exists(uid):
                raise HTTPException(404, "FileNotFoundError")
            return self.file_manager.clone_file(uid)

        @self.delete("/")
        def delete(uid: str):
            if not self.allow_delete:
                raise HTTPException(status_code=405, detail="Method not allowed")
            if not self.file_manager.file_exists(uid):
                raise HTTPException(404, "FileNotFoundError")
            return self.file_manager.delete_file(uid)

        @self.get("/")
        def get(uid: str) -> FileResponse:
            if not self.allow_get:
                raise HTTPException(status_code=405, detail="Method not allowed")
            if not self.file_manager.file_exists(uid):
                raise HTTPException(404, "FileNotFoundError")
            return FileResponse(self.file_manager.get_file(uid))

        @self.get("/hash/")
        def get_hash(uid: str):
            if not self.allow_get:
                raise HTTPException(status_code=405, detail="Method not allowed")
            if not self.file_manager.file_exists(uid):
                raise HTTPException(404, "FileNotFoundError")
            return self.file_manager.get_hash(uid)
        
        @self.get("/exists/")
        def exists(uid: str):
            if not self.allow_get:
                raise HTTPException(status_code=405, detail="Method not allowed")
            return self.file_manager.file_exists(uid)
