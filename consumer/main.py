import os

import yaml

from DicomFlowLib.fs import FileStorage
from DicomFlowLib.log.logger import CollectiveLogger
from DicomFlowLib.mq import MQSub
from docker_consumer.impl import DockerConsumer


class Main:
    def __init__(self, config):
        self.running = None
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
        self.consumer = DockerConsumer(logger=self.logger,
                                       file_storage=self.fs,
                                       pub_exchange=config["PUB_EXCHANGE"],
                                       pub_routing_key=config["PUB_ROUTING_KEY"],
                                       pub_exchange_type=config["PUB_EXCHANGE_TYPE"],
                                       pub_routing_key_as_queue=bool(config["PUB_ROUTING_KEY_AS_QUEUE"]),
                                       gpu=bool(config["GPU"])
                                       )

        self.mq = MQSub(logger=self.logger,
                        work_function=self.consumer.mq_entrypoint,
                        rabbit_hostname=config["RABBIT_HOSTNAME"],
                        rabbit_port=int(config["RABBIT_PORT"]),
                        sub_exchange=config["SUB_EXCHANGE"],
                        sub_routing_key=config["SUB_ROUTING_KEY"],
                        sub_exchange_type=config["SUB_EXCHANGE_TYPE"],
                        sub_prefetch_value=int(config["SUB_PREFETCH_COUNT"]),
                        sub_routing_key_as_queue=bool(config["SUB_ROUTING_KEY_AS_QUEUE"]) )

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
    with open("default_config.yaml", "r") as r:
        config = yaml.safe_load(r)

    for k, v in config.items():
        if k in os.environ.keys():
            config[k] = os.environ.get(k)

    m = Main(config=config)
    m.start()