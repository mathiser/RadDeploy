import logging
import os
from typing import List

from DicomFlowLib.log.mq_handler import MQHandler
from DicomFlowLib.mq import PubModel


def init_logger(name: str | None = None,
                log_dir: str = None,
                log_format: str = "%(name)s ; %(levelname)s ; %(asctime)s ; %(name)s ; %(filename)s ; %(funcName)s ; %(lineno)s ; %(message)s",
                pub_models: List[PubModel] | None = None,
                rabbit_hostname: str | None = None,
                rabbit_port: int | None = None):

    logger = logging.getLogger(name=name)
    formatter = logging.Formatter(log_format)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if log_dir: # File logger
        os.makedirs(log_dir, exist_ok=True)
        if not name: # If it is a root logger set name to root
            log_file = os.path.join(log_dir, "root" + ".log")
        else:
            log_file = os.path.join(log_dir, name + ".log")
        file_handler = logging.FileHandler(log_file, mode="a")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    if rabbit_hostname and rabbit_port and pub_models:  # Rabbit logger
        rabbit_handler = MQHandler(pub_models=pub_models, rabbit_hostname=rabbit_hostname, rabbit_port=rabbit_port)
        rabbit_handler.setFormatter(formatter)
        logger.addHandler(rabbit_handler)

    return logger
