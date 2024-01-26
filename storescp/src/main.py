import os.path
import queue
import signal
from typing import Dict

from DicomFlowLib.conf import load_configs
from DicomFlowLib.data_structures.contexts import PubModel
from DicomFlowLib.fs.file_storage_client import FileStorageClient
from DicomFlowLib.log import CollectiveLogger
from DicomFlowLib.mq import MQPub
from scp import SCP


class Main:
    def __init__(self, config: Dict):
        signal.signal(signal.SIGTERM, self.stop)
        self.running = None
        self.publish_queue = queue.Queue()
        self.logger = CollectiveLogger(name=config["LOG_NAME"],
                                       log_level=int(config["LOG_LEVEL"]),
                                       log_format=config["LOG_FORMAT"],
                                       log_dir=config["LOG_DIR"],
                                       rabbit_hostname=config["RABBIT_HOSTNAME"],
                                       rabbit_port=int(config["RABBIT_PORT"]),
                                       rabbit_password=config["RABBIT_PASSWORD"],
                                       rabbit_username=config["RABBIT_USERNAME"])

        self.fs = FileStorageClient(logger=self.logger,
                              file_storage_host=config["FILE_STORAGE_HOST"],
                              file_storage_port=config["FILE_STORAGE_PORT"])

        self.scp = SCP(logger=self.logger,
                       file_storage=self.fs,
                       publish_queue=self.publish_queue,
                       port=int(config["AE_PORT"]),
                       hostname=config["AE_HOSTNAME"],
                       tar_subdir=config["TAR_SUBDIR"],
                       pub_models=[PubModel(**d) for d in config["PUB_MODELS"]],
                       ae_title=config["AE_TITLE"],
                       pynetdicom_log_level=config["PYNETDICOM_LOG_LEVEL"])

        self.mq = MQPub(logger=self.logger,
                        publish_queue=self.publish_queue,
                        rabbit_port=int(config["RABBIT_PORT"]),
                        rabbit_hostname=config["RABBIT_HOSTNAME"])

    def start(self):
        self.running = True
        self.logger.debug("Starting")
        self.scp.start(blocking=False)
        self.logger.start()
        self.mq.start()
        while self.running:
            try:
                self.mq.join(timeout=5)
                if self.mq.is_alive():
                    pass
                else:
                    self.stop()
            except KeyboardInterrupt:
                self.stop()

    def stop(self):
        self.running = False
        self.mq.stop()
        self.scp.stop()
        self.logger.stop()

        self.mq.join()
        self.logger.join()



if __name__ == "__main__":
    config = load_configs(os.environ["CONF_DIR"])

    m = Main(config=config)
    m.start()
