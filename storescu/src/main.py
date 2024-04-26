import logging
import os
import signal
import time

from DicomFlowLib.conf import load_configs
from DicomFlowLib.log import init_logger
from DicomFlowLib.log.mq_handler import MQHandler
from DicomFlowLib.mq import PubModel, SubModel
from DicomFlowLib.fs import FileStorageClient
from DicomFlowLib.mq import MQSub
from storescu import STORESCU


class Main:
    def __init__(self, config):
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

        self.fs = FileStorageClient(file_storage_url=config["FILE_STORAGE_URL"],
                                    log_level=int(config["LOG_LEVEL"]))

        self.scu = STORESCU(file_storage=self.fs,
                            ae_title=config["AE_TITLE"],
                            ae_port=int(config["AE_PORT"]),
                            ae_hostname=config["AE_HOSTNAME"],
                            log_level=int(config["LOG_LEVEL"]),
                            )

        self.mq = MQSub(rabbit_hostname=config["RABBIT_HOSTNAME"], rabbit_port=int(config["RABBIT_PORT"]),
                        sub_models=[SubModel(**d) for d in config["SUB_MODELS"]],
                        pub_models=[PubModel(**d) for d in config["PUB_MODELS"]],
                        work_function=self.scu.mq_entrypoint,
                        sub_prefetch_value=int(config["SUB_PREFETCH_COUNT"]),
                        sub_queue_kwargs=config["SUB_QUEUE_KWARGS"],
                        log_level=int(config["LOG_LEVEL"]))

    def start(self, blocking=True):
        self.running = True
        self.mq.start()
        self.mq_handler.start()
        self.logger.debug("Started SCU")

        while self.running and not blocking:
            time.sleep(1)

    def stop(self, signalnum=None, stack_frame=None):
        self.logger.debug("Stopping SCU")
        self.running = False

        self.mq.stop()
        self.mq_handler.stop()

        self.mq.join()
        self.mq_handler.join()


if __name__ == "__main__":
    config = load_configs(os.environ["CONF_DIR"], os.environ["CURRENT_CONF"])

    m = Main(config=config)
    m.start()
