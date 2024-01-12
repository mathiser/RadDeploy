import os
import signal

from DicomFlowLib.conf import load_configs
from DicomFlowLib.data_structures.contexts.pub_context import SubModel, PubModel
from DicomFlowLib.log import CollectiveLogger
from DicomFlowLib.mq import MQSub
from fingerprinter import Fingerprinter


class Main:
    def __init__(self, config):
        signal.signal(signal.SIGTERM, self.stop)

        self.running = True

        self.logger = CollectiveLogger(name=config["LOG_NAME"],
                                       log_level=int(config["LOG_LEVEL"]),
                                       log_format=config["LOG_FORMAT"],
                                       log_dir=config["LOG_DIR"],
                                       rabbit_hostname=config["RABBIT_HOSTNAME"],
                                       rabbit_port=int(config["RABBIT_PORT"]),
                                       rabbit_password=config["RABBIT_PASSWORD"],
                                       rabbit_username=config["RABBIT_USERNAME"])

        self.fp = Fingerprinter(logger=self.logger,
                                pub_models=[PubModel(**d) for d in config["PUB_MODELS"]],
                                flow_directory=config["FLOW_DIRECTORY"])

        self.mq = MQSub(logger=self.logger,
                        work_function=self.fp.mq_entrypoint,
                        rabbit_hostname=config["RABBIT_HOSTNAME"],
                        rabbit_port=int(config["RABBIT_PORT"]),
                        sub_models=[SubModel(**d) for d in config["SUB_MODELS"]],
                        sub_prefetch_value=int(config["SUB_PREFETCH_COUNT"]),
                        sub_queue_name=config["SUB_QUEUE_NAME"])

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
    config = load_configs(os.environ["CONF_DIR"])

    m = Main(config=config)
    m.start()