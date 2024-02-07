import logging
import os
from typing import List

from DicomFlowLib.log.mq_handler import MQHandler
from DicomFlowLib.mq import PubModel


def init_logger(name: str,
                log_dir: str,
                log_format: str,
                pub_models: List[PubModel] | None = None,
                rabbit_hostname: str | None = None,
                rabbit_port: int | None = None):
    logger = logging.getLogger()
    formatter = logging.Formatter(log_format)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, name + ".log")
    file_handler = logging.FileHandler(log_file, mode="a")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if rabbit_hostname and rabbit_port and pub_models:
        rabbit_handler = MQHandler(pub_models=pub_models, rabbit_hostname=rabbit_hostname, rabbit_port=rabbit_port)
        rabbit_handler.setFormatter(formatter)
        logger.addHandler(rabbit_handler)
