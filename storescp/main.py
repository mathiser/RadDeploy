import os.path
import queue

import yaml
from pydantic.v1.utils import deep_update

from DicomFlowLib.default_config import Config
from DicomFlowLib.fs.file_storage import FileStorage
from DicomFlowLib.log.logger import CollectiveLogger
from DicomFlowLib.mq import MQPub
from scp import SCP

class Main:
    def __init__(self, config):
        self.running = True
        self.publish_queue = queue.Queue()
        self.logger = CollectiveLogger(**config["logger"])
        self.fs = FileStorage(logger=self.logger, **config["file_storage"])
        self.scp = SCP(logger=self.logger,
                       file_storage=self.fs,
                       publish_queue=self.publish_queue,
                       **config["scp"])
        self.mq = MQPub(logger=self.logger,
                        publish_queue=self.publish_queue,
                        **config["mq_pub"])

    def start(self):
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
    config = Config["storescp"]

    user_config_path = "config/config.yaml"
    if os.path.isfile(user_config_path):
        with open(user_config_path, "r") as r:
            user_config = yaml.safe_load(r)
        config = deep_update(config, user_config)

    m = Main(config=config)
    m.start()
