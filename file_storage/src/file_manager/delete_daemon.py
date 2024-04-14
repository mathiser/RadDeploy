import logging
import os
import threading
import time

class DeleteDaemon(threading.Thread):
    def __init__(self,
                 base_dir: str,
                 delete_files_after,
                 delete_run_interval,
                 log_level: int = 20):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)

        self.base_dir = base_dir
        self.delete_files_after = delete_files_after
        self.delete_run_interval = delete_run_interval
        self.running = False

    def run(self):
        self.running = True
        while self.running:
            for fol, subs, files in os.walk(self.base_dir):
                for file in files:
                    p = os.path.join(fol, file)
                    if time.time() - os.path.getatime(p) > self.delete_files_after:
                        self.logger.debug(f"Timeout for file uid: {p} - deleting!")
                        os.remove(p)

            time.sleep(self.delete_run_interval)

    def stop(self):
        self.running = False
