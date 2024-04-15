import queue
import time
from typing import Tuple

import docker
import pika
import pytest

from DicomFlowLib.data_structures.contexts import PublishContext
from DicomFlowLib.fs.client.mock_impl import MockFileStorageClient
from DicomFlowLib.mq import MQPub, PubModel, MQBase
from scp import SCP
from scp.models import SCPAssociation
from scp_release_handler.impl import SCPReleaseHandler
from .test_scp import post


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
                                 ports={5672: 5677})
        time.sleep(15)
        yield container
    cli.close()


@pytest.fixture
def fs():
    return MockFileStorageClient()


def test_end_to_end(mq_container, fs):
    exchange_name = "SCP_TEST"
    mq_base = MQBase(rabbit_hostname="localhost", rabbit_port=5677, close_conn_on_exit=True, log_level=10).connect()
    mq_base.setup_exchange(exchange_name, "topic")
    q = mq_base.setup_queue_and_bind(exchange_name, "#")

    scp_out_queue: queue.Queue[SCPAssociation] = queue.Queue()
    scp = SCP(
        out_queue=scp_out_queue,
        port=10000,
        hostname="localhost",
        blacklist_networks=None,
        whitelist_networks=None,
        ae_title="DicomFlow",
        pynetdicom_log_level=20,
        log_level=10).start(blocking=False)

    post(0,
         ip=scp.hostname,
         port=scp.port,
         ae_title=scp.ae_title)

    time.sleep(1)
    assert scp.out_queue.qsize() == 1

    scp_release_handler_out_queue: queue.Queue[Tuple[PubModel, PublishContext]] = queue.Queue()
    scp_release_handler = SCPReleaseHandler(
        file_storage=fs,
        pub_models=[PubModel(exchange=exchange_name)],
        in_queue=scp_out_queue,
        out_queue=scp_release_handler_out_queue
    )
    scp_release_handler.start()

    time.sleep(1)
    assert scp_release_handler.out_queue.qsize() == 1

    mq = MQPub(rabbit_port=5677,
               rabbit_hostname="localhost",
               log_level=10,
               in_queue=scp_release_handler_out_queue)
    mq.start()

    time.sleep(1)
    method_frame, header_frame, body = mq_base._channel.basic_get(q)

    assert method_frame

    print(method_frame, header_frame, body)
    mq_base._channel.basic_ack(method_frame.delivery_tag)

    scp.stop()
    scp_release_handler.stop()
    mq.stop()