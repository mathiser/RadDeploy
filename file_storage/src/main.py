import os

from DicomFlowLib.conf import load_configs
from DicomFlowLib.log import CollectiveLogger
from file_storage import FileStorage


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

        self.fs = FileStorage(logger=self.logger,
                              file_storage_host=config["FILE_STORAGE_HOST"],
                              file_storage_port=config["FILE_STORAGE_PORT"],
                              base_dir=config["FILE_STORAGE_BASE_DIR"],
                              suffix=config["FILE_STORAGE_SUFFIX"])

    def start(self):
        self.fs.start()

if __name__ == "__main__":
    config = load_configs(os.environ["CONF_DIR"])

    m = Main(config=config)
    m.start()
