import os

from DicomFlowLib.conf import load_configs
from DicomFlowLib.log import CollectiveLogger
from DicomFlowLib.fs import FileStorageServer


class Main:
    def __init__(self, config):
        self.running = False
        self.logger = CollectiveLogger(name=config["LOG_NAME"],
                                       log_level=int(config["LOG_LEVEL"]),
                                       log_format=config["LOG_FORMAT"],
                                       log_dir=config["LOG_DIR"],
                                       rabbit_hostname=config["RABBIT_HOSTNAME"],
                                       rabbit_port=int(config["RABBIT_PORT"]),
                                       rabbit_password=config["RABBIT_PASSWORD"],
                                       rabbit_username=config["RABBIT_USERNAME"])

        self.fs = FileStorageServer(logger=self.logger,
                                    host=config["FILE_STORAGE_HOST"],
                                    port=config["FILE_STORAGE_PORT"],
                                    base_dir=config["FILE_STORAGE_BASE_DIR"],
                                    suffix=config["FILE_STORAGE_SUFFIX"],
                                    allow_get=True,
                                    allow_clone=False,
                                    allow_post=False,
                                    allow_delete=False)

    def start(self):
        self.fs.start()


if __name__ == "__main__":
    config = load_configs(os.environ["CONF_DIR"])

    m = Main(config=config)
    m.start()
