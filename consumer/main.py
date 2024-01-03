import os

import yaml
from pydantic.v1.utils import deep_update

from DicomFlowLib.default_config import Config
from DicomFlowLib.fs import FileStorage
from DicomFlowLib.log.logger import CollectiveLogger
from DicomFlowLib.mq import MQSub
from docker_consumer.impl import DockerConsumer


class Main:
    def __init__(self, config):
        self.running = None
        self.logger = CollectiveLogger(**config["logger"])

        self.fs = FileStorage(logger=self.logger,
                              **config["file_storage"])
        self.consumer = DockerConsumer(logger=self.logger,
                                       file_storage=self.fs,
                                       **config["consumer"])

        self.mq = MQSub(logger=self.logger,
                        **config["mq_sub"],
                        work_function=self.consumer.mq_entrypoint)

    def start(self):
        self.running = True

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

        self.mq.join()
        self.logger.join()


if __name__ == "__main__":
    config = Config["consumer"]
    user_config_path = "config/config.yaml"
    if os.path.isfile(user_config_path):
        with open(user_config_path, "r") as r:
            user_config = yaml.safe_load(r)
        config = deep_update(config, user_config)

    m = Main(config=config)
    m.start()
