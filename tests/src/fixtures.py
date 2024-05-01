import logging
import os

import pytest
import yaml

from RadDeployLib.fs import FileStorageClient
from RadDeployLib.log import init_logger
from RadDeployLib.mq import MQBase


@pytest.fixture
def config(tmpdir):
    with open(os.path.join(os.path.dirname(__file__), "..", "conf.d", "00_default_config.yaml"), "r") as r:
        config = yaml.safe_load(r)

    return config

@pytest.fixture
def logger(config):
    logger = logging.getLogger(__name__)
    logger.setLevel(int(config["LOG_LEVEL"]))
    return logger


@pytest.fixture
def mq_base_real(config):
    return MQBase(rabbit_hostname=config["RABBIT_HOSTNAME"],
                  rabbit_port=int(config["RABBIT_PORT"]),
                  log_level=int(config["LOG_LEVEL"])).connect()


@pytest.fixture
def fs_real(config):
    return FileStorageClient(file_storage_url=config["FILE_STORAGE_URL"])
