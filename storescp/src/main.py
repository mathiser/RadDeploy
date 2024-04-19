import logging
import os.path
import queue
import signal
import time
from typing import Dict, Tuple, Type

from DicomFlowLib.conf import load_configs
from DicomFlowLib.mq.mq_models import PublishContext
from DicomFlowLib.fs.client.interface import FileStorageClientInterface
from DicomFlowLib.log.mq_handler import MQHandler
from DicomFlowLib.mq import PubModel
from DicomFlowLib.fs.client.impl import FileStorageClient
from DicomFlowLib.log import init_logger
from DicomFlowLib.mq import MQPub
from scp import SCP
from storescp.src.scp.models import SCPAssociation
from storescp.src.scp_release_handler.impl import SCPReleaseHandler


class Main:
    def __init__(self,
                 config: Dict,
                 FileStorageClientImpl: Type[FileStorageClientInterface] = FileStorageClient):
        signal.signal(signal.SIGTERM, self.stop)
        self.running = False

        self.publish_queue = queue.Queue()
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

        self.scp_out_queue: queue.Queue[SCPAssociation] = queue.Queue()
        self.scp = SCP(
            out_queue=self.scp_out_queue,
            port=int(config["AE_PORT"]),
            hostname=config["AE_HOSTNAME"],
            blacklist_networks=config["AE_BLACKLISTED_IP_NETWORKS"],
            whitelist_networks=config["AE_WHITELISTED_IP_NETWORKS"],
            ae_title=config["AE_TITLE"],
            pynetdicom_log_level=int(config["PYNETDICOM_LOG_LEVEL"]),
            log_level=int(config["LOG_LEVEL"]))

        self.fs = FileStorageClientImpl(file_storage_url=config["FILE_STORAGE_URL"],
                                        log_level=int(config["LOG_LEVEL"]))

        self.scp_release_handler_out_queue: queue.Queue[Tuple[PubModel, PublishContext]] = queue.Queue()
        self.scp_release_handler = SCPReleaseHandler(
            file_storage=self.fs,
            pub_models=[PubModel(**d) for d in config["PUB_MODELS"]],
            in_queue=self.scp_out_queue,
            out_queue=self.scp_release_handler_out_queue
        )

        self.mq = MQPub(rabbit_port=int(config["RABBIT_PORT"]),
                        rabbit_hostname=config["RABBIT_HOSTNAME"],
                        log_level=int(config["LOG_LEVEL"]),
                        in_queue=self.scp_release_handler_out_queue)

    def start(self, blocking=True):
        self.logger.debug("Starting")
        self.running = True
        self.mq_handler.start()
        self.mq.start()
        self.scp_release_handler.start()
        self.scp.start(blocking=False)

        while self.running and blocking:
            time.sleep(1)

    def stop(self, signalnum=None, stack_frame=None):
        self.running = False
        self.mq.stop()
        self.scp_release_handler.stop()
        self.mq_handler.stop()

        self.mq.join()
        self.scp_release_handler.join()
        self.mq_handler.stop()


if __name__ == "__main__":
    config = load_configs(os.environ["CONF_DIR"], os.environ["CURRENT_CONF"])
    m = Main(config=config)
    m.start()
