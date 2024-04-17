import logging
import os
import signal
import time
from typing import Type

from DicomFlowLib.conf import load_configs
from DicomFlowLib.fs.client.interface import FileStorageClientInterface
from DicomFlowLib.log import init_logger
from DicomFlowLib.log.mq_handler import MQHandler
from DicomFlowLib.mq import PubModel, SubModel
from DicomFlowLib.fs import FileStorageClient

from DicomFlowLib.mq import MQSub
from fingerprinting import Fingerprinter


class Main:
    def __init__(self,
                 config,
                 FileStorageClass: Type[FileStorageClientInterface] = FileStorageClient):
        signal.signal(signal.SIGTERM, self.stop)

        self.running = False

        self.mq_handler = MQHandler(
            rabbit_hostname=config["RABBIT_HOSTNAME"],
            rabbit_port=int(config["RABBIT_PORT"]),
            pub_models=[PubModel(**d) for d in config["LOG_PUB_MODELS"]]
        )
        init_logger(name=None,  # init root logger,
                    log_format=config["LOG_FORMAT"],
                    log_dir=config["LOG_DIR"],
                    mq_handler=self.mq_handler)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(int(config["LOG_LEVEL"]))

        self.fs = FileStorageClass(file_storage_url=config["FILE_STORAGE_URL"],
                                   log_level=int(config["LOG_LEVEL"]))

        self.fp = Fingerprinter(file_storage=self.fs,
                                flow_directory=config["FLOW_DIRECTORY"],
                                log_level=int(config["LOG_LEVEL"]))

        self.mq = MQSub(work_function=self.fp.mq_entrypoint,
                        rabbit_hostname=config["RABBIT_HOSTNAME"],
                        rabbit_port=int(config["RABBIT_PORT"]),
                        pub_models=[PubModel(**d) for d in config["PUB_MODELS"]],
                        sub_models=[SubModel(**d) for d in config["SUB_MODELS"]],
                        sub_prefetch_value=int(config["SUB_PREFETCH_COUNT"]),
                        sub_queue_kwargs=config["SUB_QUEUE_KWARGS"],
                        log_level=int(config["LOG_LEVEL"]))

    def start(self):
        self.running = True
        self.mq.start()

    def stop(self, signalnum=None, stack_frame=None):
        self.running = False
        self.mq.stop()
        self.mq.join()
        self.mq_handler.stop()  # Also joins


if __name__ == "__main__":
    config = load_configs(os.environ["CONF_DIR"], os.environ["CURRENT_CONF"])

    m = Main(config=config)
    m.start()
