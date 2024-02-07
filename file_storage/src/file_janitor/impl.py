import logging
import os
import threading
import time

from DicomFlowLib.fs import FileStorageServer


class FileJanitor(threading.Thread):
    def __init__(self, file_storage: FileStorageServer, delete_files_after: int, run_inteval: int = 60):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.fs = file_storage
        self.base_dir = file_storage.base_dir
        self.delete_files_after = delete_files_after
        self.run_inteval = run_inteval
        self.running = False

    def run(self):
        self.running = True
        while self.running:
            for fol, subs, files in os.walk(self.base_dir):
                for file in files:
                    p = os.path.join(fol, file)
                    uid = self.fs.get_uid_from_path(p)
                    if time.time() - os.path.getatime(p) > self.delete_files_after:
                        self.logger.debug(f"Timeout for file uid: {uid} - deleting!")
                        self.fs.delete_file(uid)

            time.sleep(self.run_inteval)

    def stop(self):
        self.running = False