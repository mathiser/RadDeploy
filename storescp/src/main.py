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
                                       pub_models=[PubModel(**d) for d in config["LOG_PUB_MODELS"]])

        self.fs = FileStorageClient(logger=self.logger,
                                    file_storage_url=config["FILE_STORAGE_URL"])
        self.mq = MQPub(logger=self.logger,
                        rabbit_port=int(config["RABBIT_PORT"]),
                        rabbit_hostname=config["RABBIT_HOSTNAME"])
        self.scp = SCP(logger=self.logger,
                       file_storage=self.fs,
                       port=int(config["AE_PORT"]),
                       hostname=config["AE_HOSTNAME"],
                       tar_subdir=config["TAR_SUBDIR"],
                       pub_models=[PubModel(**d) for d in config["PUB_MODELS"]],
                       ae_title=config["AE_TITLE"],
                       mq_pub=self.mq,
                       pynetdicom_log_level=config["PYNETDICOM_LOG_LEVEL"],
                       routing_key_success=config["PUB_ROUTING_KEY_SUCCESS"],
                       routing_key_fail=config["PUB_ROUTING_KEY_FAIL"]
                       )



    def start(self):
        self.running = True
        self.logger.debug("Starting")
        self.scp.start(blocking=False)
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

    def stop(self, signalnum=None, stack_frame=None):
        self.running = False
        self.mq.stop()
        self.scp.stop()
        self.logger.stop()

        self.mq.join()



if __name__ == "__main__":
    config = load_configs(os.environ["CONF_DIR"])

    m = Main(config=config)
    m.start()
