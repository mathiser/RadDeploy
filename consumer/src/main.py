import os
import signal

from DicomFlowLib.conf import load_configs
from DicomFlowLib.data_structures.contexts import PubModel, SubModel

from DicomFlowLib.fs import FileStorageClient
from DicomFlowLib.log import CollectiveLogger
from DicomFlowLib.mq import MQSub
from docker_consumer.impl import DockerConsumer


class Main:
    def __init__(self, config):
        signal.signal(signal.SIGTERM, self.stop)

        self.running = None
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

        self.consumer = DockerConsumer(logger=self.logger,
                                       file_storage=self.fs,
                                       gpus=config["GPUS"])

        self.mq = MQSub(logger=self.logger,
                        work_function=self.consumer.mq_entrypoint,
                        rabbit_hostname=config["RABBIT_HOSTNAME"],
                        rabbit_port=int(config["RABBIT_PORT"]),
                        sub_models=[SubModel(**d) for d in config["SUB_MODELS"]],
                        sub_prefetch_value=int(config["SUB_PREFETCH_COUNT"]),
                        sub_queue_kwargs=config["SUB_QUEUE_KWARGS"],
                        pub_models=[PubModel(**d) for d in config["PUB_MODELS"]],
                        )

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
    config = load_configs(os.environ["CONF_DIR"])

    m = Main(config=config)
    m.start()
