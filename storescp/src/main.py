import logging
import os.path
import queue
import signal
from typing import Dict

from DicomFlowLib.conf import load_configs
from DicomFlowLib.mq import PubModel
from DicomFlowLib.fs.client.impl import FileStorageClient
from DicomFlowLib.log import init_logger
from DicomFlowLib.mq import MQPub
from scp import SCP
from storescp.src.scp_release_handler.impl import SCPReleaseHandler


class Main:
    def __init__(self, config: Dict):
        signal.signal(signal.SIGTERM, self.stop)

        self.running = None
        self.publish_queue = queue.Queue()
        init_logger(name=None,  # init root logger,
                    log_format=config["LOG_FORMAT"],
                    log_dir=config["LOG_DIR"],
                    rabbit_hostname=config["RABBIT_HOSTNAME"],
                    rabbit_port=int(config["RABBIT_PORT"]),
                    pub_models=[PubModel(**d) for d in config["LOG_PUB_MODELS"]])
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(int(config["LOG_LEVEL"]))

        self.fs = FileStorageClient(file_storage_url=config["FILE_STORAGE_URL"],
                                    log_level=int(config["LOG_LEVEL"]))

        self.mq = MQPub(rabbit_port=int(config["RABBIT_PORT"]),
                        rabbit_hostname=config["RABBIT_HOSTNAME"],
                        log_level=int(config["LOG_LEVEL"]))
        self.scp_release_handler = SCPReleaseHandler(pub_models=[PubModel(**d) for d in config["PUB_MODELS"]],
                                                     file_storage=self.fs,
                                                     routing_key_success=config["PUB_ROUTING_KEY_SUCCESS"],
                                                     routing_key_fail=config["PUB_ROUTING_KEY_FAIL"],
                                                     mq_pub=self.mq,
                                                     )
        self.scp = SCP(
            scp_release_handler=self.scp_release_handler,
            port=int(config["AE_PORT"]),
            hostname=config["AE_HOSTNAME"],
            blacklist_networks=config["AE_BLACKLISTED_IP_NETWORKS"],
            whitelist_networks=config["AE_WHITELISTED_IP_NETWORKS"],
            ae_title=config["AE_TITLE"],
            pynetdicom_log_level=int(config["PYNETDICOM_LOG_LEVEL"]),
            log_level=int(config["LOG_LEVEL"]))

    def start(self):
        self.running = True
        self.mq.start()

        self.scp.start(blocking=False)
        self.logger.debug("Starting")

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
        self.running = False
        self.mq.stop()
        self.scp.stop()
        self.mq.join()


if __name__ == "__main__":
    config = load_configs(os.environ["CONF_DIR"], os.environ["CURRENT_CONF"])
    m = Main(config=config)
    m.start()
