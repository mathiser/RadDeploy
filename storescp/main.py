import os.path
import queue
from typing import Dict

import yaml

from DicomFlowLib.fs.file_storage import FileStorage
from DicomFlowLib.log.logger import CollectiveLogger
from DicomFlowLib.mq import MQPub
from scp import SCP


class Main:
    def __init__(self, config: Dict):
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

        self.fs = FileStorage(logger=self.logger,
                              base_dir=config["FILE_STORAGE_BASE_DIR"])
        self.scp = SCP(logger=self.logger,
                       file_storage=self.fs,
                       publish_queue=self.publish_queue,
                       port=int(config["AE_PORT"]),
                       hostname=config["AE_HOSTNAME"],
                       tar_subdir=config["TAR_SUBDIR"],
                       pub_exchange=config["PUB_EXCHANGE"],
                       pub_routing_key=config["PUB_ROUTING_KEY"],
                       pub_exchange_type=config["PUB_EXCHANGE_TYPE"],
                       ae_title=config["AE_TITLE"],
                       pub_routing_key_as_queue=config["PUB_ROUTING_KEY_AS_QUEUE"],
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
    with open("default_config.yaml", "r") as r:
        config = yaml.safe_load(r)
    
    for k, v in config.items():
        if k in os.environ.keys():
            config[k] = os.environ.get(k)
        
    m = Main(config=config)
    m.start()
