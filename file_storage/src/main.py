import os

from DicomFlowLib.conf import load_configs
from DicomFlowLib.mq import PubModel
from DicomFlowLib.log import CollectiveLogger
from DicomFlowLib.fs import FileStorageServer
from file_janitor import FileJanitor


class Main:
    def __init__(self, config):
        self.running = False
        self.logger = CollectiveLogger(name=config["LOG_NAME"],
                                       log_level=int(config["LOG_LEVEL"]),
                                       log_format=config["LOG_FORMAT"],
                                       log_dir=config["LOG_DIR"],
                                       rabbit_hostname=config["RABBIT_HOSTNAME"],
                                       rabbit_port=int(config["RABBIT_PORT"]),
                                       pub_models=[PubModel(**m) for m in config["LOG_PUB_MODELS"]])

        self.fs = FileStorageServer(logger=self.logger,
                                    host=config["FILE_STORAGE_HOST"],
                                    port=config["FILE_STORAGE_PORT"],
                                    base_dir=config["FILE_STORAGE_BASE_DIR"],
                                    suffix=config["FILE_STORAGE_SUFFIX"],
                                    delete_on_get=False)

        self.fj = FileJanitor(file_storage=self.fs,
                              logger=self.logger,
                              delete_files_after=int(config["FILE_JANITOR_DELETE_FILES_AFTER"]),
                              run_inteval=int(config["FILE_JANITOR_RUN_INTERVAL"]))
    def start(self):
        self.fj.start()
        self.fs.start()

if __name__ == "__main__":
    config = load_configs(os.environ["CONF_DIR"], os.environ["CURRENT_CONF"])

    m = Main(config=config)
    m.start()
