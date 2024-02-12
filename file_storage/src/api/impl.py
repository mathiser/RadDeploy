import logging
from typing import Dict

import uvicorn
from fastapi import FastAPI

from DicomFlowLib.fs import FileStorageRouter


class FileStorageServer(FastAPI):
    def __init__(self, host: str, port: int, file_managers: Dict, log_level: int = 20, **extra):
        super().__init__(**extra)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        self.host = host
        self.port = port
        for prefix, file_manager_kwargs in file_managers.items():
            router = FileStorageRouter(**file_manager_kwargs)
            self.include_router(router, prefix=prefix)

    def start(self):
        uvicorn.run(app=self, host=self.host, port=self.port)