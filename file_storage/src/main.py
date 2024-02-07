import logging
import os

from DicomFlowLib.conf import load_configs
from DicomFlowLib.log import init_logger
from DicomFlowLib.mq import PubModel
from DicomFlowLib.fs import FileStorageServer
from file_janitor import FileJanitor


class Main:
    def __init__(self, config):
        self.running = False
        init_logger(name=config["LOG_NAME"],
                    log_format=config["LOG_FORMAT"],
                    log_dir=config["LOG_DIR"],
                    rabbit_hostname=config["RABBIT_HOSTNAME"],
                    rabbit_port=int(config["RABBIT_PORT"]),
                    pub_models=[PubModel(**m) for m in config["LOG_PUB_MODELS"]])

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(int(config["LOG_LEVEL"]))

        self.fs = FileStorageServer(host=config["FILE_STORAGE_HOST"],
                                    port=config["FILE_STORAGE_PORT"],
                                    base_dir=config["FILE_STORAGE_BASE_DIR"],
                                    suffix=config["FILE_STORAGE_SUFFIX"],
                                    delete_on_get=False,
                                    log_level=int(config["LOG_LEVEL"]))

        self.fj = FileJanitor(file_storage=self.fs,
                              delete_files_after=int(config["FILE_JANITOR_DELETE_FILES_AFTER"]),
                              run_inteval=int(config["FILE_JANITOR_RUN_INTERVAL"]),
                              log_level=int(config["LOG_LEVEL"]))

    def start(self):
        self.fj.start()
        self.fs.start()


if __name__ == "__main__":
    config = load_configs(os.environ["CONF_DIR"], os.environ["CURRENT_CONF"])

    m = Main(config=config)
    m.start()
