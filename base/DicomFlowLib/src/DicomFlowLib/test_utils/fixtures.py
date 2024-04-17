import os, inspect
from io import BytesIO

import docker
import pytest
import time

from DicomFlowLib.mq import MQBase
from DicomFlowLib.test_utils.mock_classes import MockFileStorageClient


@pytest.fixture
def mq_container():
    test_container_name = "test_rabbit"
    cli = docker.from_env()
    for container in cli.containers.list():
        if container.name == test_container_name:
            yield container
            break
    else:
        container = cli.containers.run(name=test_container_name,
                                       image="rabbitmq:3-management",
                                       remove=True,
                                       ports={5672: 5677,
                                              15672: 15677})
        time.sleep(15)
        yield container
    cli.close()


@pytest.fixture
def fs():
    return MockFileStorageClient()


@pytest.fixture
def mq_base():
    return MQBase(rabbit_hostname="localhost", rabbit_port=5677, close_conn_on_exit=True, log_level=10).connect()


@pytest.fixture
def flow_dir():
    from DicomFlowLib.test_utils.test_data import flows
    return os.path.dirname(flows.__file__)


@pytest.fixture
def scan_dir():
    from DicomFlowLib.test_utils.test_data import scans
    return os.path.dirname(scans.__file__)


@pytest.fixture
def scp_tar_path():
    from DicomFlowLib.test_utils.test_data import scp_tar
    return os.path.join(os.path.dirname(scp_tar.__file__), "scp.tar")

@pytest.fixture
def scp_tar(scp_tar_path):
    with open(scp_tar_path, "rb") as scp_tar_file:
        file = BytesIO(scp_tar_file.read())
    file.seek(0)
    return file