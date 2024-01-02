import os

import yaml
from pydantic.v1.utils import deep_update

from DicomFlowLib.default_config import Config
from DicomFlowLib.log.logger import CollectiveLogger
from DicomFlowLib.mq import MQSub
from fingerprinter import Fingerprinter


class Main:
    def __init__(self, config=Config):
        self.running = True

        self.logger = CollectiveLogger(**config["logger"])
        self.fp = Fingerprinter(logger=self.logger,
                                **config["fingerprinter"])
        self.mq = MQSub(logger=self.logger,
                        **config["mq_sub"],
                        work_function=self.fp.mq_entrypoint)

    def start(self):
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
        self.logger.stop()


if __name__ == "__main__":
    config = Config["fingerprinter"]
    config["logger"] = Config["logger"]
    config["logger"]["name"] = "fingerprinter"

    config["file_storage"] = Config["file_storage"]

    if os.path.isfile("./config.yaml"):
        with open("config.yaml", "r") as r:
            user_config = yaml.safe_load(r)
        config = deep_update(config, user_config)
    m = Main(config=config)
    m.start()