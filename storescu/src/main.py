import logging
import os
import signal

from DicomFlowLib.conf import load_configs
from DicomFlowLib.log import init_logger
from DicomFlowLib.mq import PubModel, SubModel
from DicomFlowLib.fs import FileStorageClient
from DicomFlowLib.mq import MQSub
from storescu import STORESCU


class Main:
    def __init__(self, config):
        signal.signal(signal.SIGTERM, self.stop)
        self.running = False
        init_logger(name=config["LOG_NAME"],
                    log_format=config["LOG_FORMAT"],
                    log_dir=config["LOG_DIR"],
                    rabbit_hostname=config["RABBIT_HOSTNAME"],
                    rabbit_port=int(config["RABBIT_PORT"]),
                    pub_models=[PubModel(**d) for d in config["LOG_PUB_MODELS"]])

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(int(config["LOG_LEVEL"]))

        self.fs = FileStorageClient(file_storage_url=config["FILE_STORAGE_URL"],
                                    log_level=int(config["LOG_LEVEL"]))

        self.scu = STORESCU(file_storage=self.fs,
                            pub_routing_key_success=config["PUB_ROUTING_KEY_SUCCESS"],
                            pub_routing_key_fail=config["PUB_ROUTING_KEY_FAIL"],
                            ae_title=config["AE_TITLE"],
                            ae_port=int(config["AE_PORT"]),
                            ae_hostname=config["AE_HOSTNAME"],
                            log_level=int(config["LOG_LEVEL"]))

        self.mq = MQSub(rabbit_hostname=config["RABBIT_HOSTNAME"], rabbit_port=int(config["RABBIT_PORT"]),
                        sub_models=[SubModel(**d) for d in config["SUB_MODELS"]],
                        pub_models=[PubModel(**d) for d in config["PUB_MODELS"]], work_function=self.scu.mq_entrypoint,
                        sub_prefetch_value=int(config["SUB_PREFETCH_COUNT"]),
                        sub_queue_kwargs=config["SUB_QUEUE_KWARGS"],
                        pub_routing_key_error=config["PUB_ROUTING_KEY_ERROR"],
                        log_level=int(config["LOG_LEVEL"]))

    def start(self):
        self.logger.debug("Starting SCU")
        self.running = True

        self.mq.start()
        self.logger.debug("Starting SCU")

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
        self.logger.debug("Stopping SCU")
        self.running = False
        self.mq.stop()
        self.mq.join()
        self.logger.debug("Stopping SCU")


if __name__ == "__main__":
    config = load_configs(os.environ["CONF_DIR"], os.environ["CURRENT_CONF"])

    m = Main(config=config)
    m.start()
