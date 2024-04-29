import logging
import os

from RadDeployLib.conf import load_configs
from RadDeployLib.log.mq_handler import MQHandler
from file_manager import FileManager
from RadDeployLib.log import init_logger
from RadDeployLib.mq import PubModel
from api import FileStorageServer


class Main:
    def __init__(self, config):
        self.running = False
        self.mq_handler = MQHandler(
            rabbit_hostname=config["RABBIT_HOSTNAME"],
            rabbit_port=int(config["RABBIT_PORT"]),
            pub_models=[PubModel(**m) for m in config["LOG_PUB_MODELS"]]
        )
        init_logger(name=None,  # init root logger,
                    log_format=config["LOG_FORMAT"],
                    log_dir=config["LOG_DIR"],
                    mq_handler=self.mq_handler)

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(int(config["LOG_LEVEL"]))
        routers = {
            "/files": {
                "file_manager": FileManager(log_level=int(config["LOG_LEVEL"]),
                                            base_dir=config["FILE_STORAGE_TEMP_DIR"],
                                            delete_files_after=int(config["FILE_JANITOR_DELETE_FILES_AFTER"]),
                                            delete_run_interval=int(config["FILE_JANITOR_RUN_INTERVAL"]))
            },
            "/static": {
                "file_manager": FileManager(log_level=int(config["LOG_LEVEL"]),
                                            base_dir=config["FILE_STORAGE_STATIC_DIR"]),
                "allow_post": False,
                "allow_clone": False,
                "allow_delete": False
            }
        }

        self.fs = FileStorageServer(host=config["FILE_STORAGE_HOST"],
                                    port=int(config["FILE_STORAGE_PORT"]),
                                    log_level=int(config["LOG_LEVEL"]),
                                    file_managers=routers)

    def start(self):
        self.fs.start()


if __name__ == "__main__":
    config = load_configs(os.environ["CONF_DIR"], os.environ["CURRENT_CONF"])

    m = Main(config=config)
    m.start()
