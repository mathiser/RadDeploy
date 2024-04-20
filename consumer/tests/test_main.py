import time

import pytest
from main import Main

from DicomFlowLib.data_structures.flow import Model
from DicomFlowLib.data_structures.service_contexts import PendingModelContext
from DicomFlowLib.test_utils.mock_classes import MockFileStorageClient
from DicomFlowLib.test_utils.fixtures import *

@pytest.fixture
def config(tmpdir):
    return {
        # RabbitMQ
        "RABBIT_HOSTNAME": "localhost",
        "RABBIT_PORT": 5677,
        "LOG_LEVEL": 10,
        "LOG_FORMAT": "%(name)s ; %(levelname)s ; %(asctime)s ; %(name)s ; %(filename)s ; %(funcName)s ; %(lineno)s ; %(message)s",
        "LOG_PUB_MODELS": [
            {"exchange": "logs"},
        ],
        "LOG_DIR": tmpdir,
        "CPU_SUB_QUEUE_KWARGS": {
            "queue": "SCHEDULED_CPU",
            "passive": False,
            "durable": False,
            "exclusive": False,
            "auto_delete": False,
        },
        "SUB_PREFETCH_COUNT": 1,
        "CPU_SUB_MODELS": [
            {
                "exchange": "TEST_CONSUMER_IN",
                "exchange_type": "topic",
                "routing_keys": ["SCHEDULED_CPU"],
                "routing_key_fetch_echo": None,
            },
        ],
        "PUB_MODELS": [
            {
                "exchange": "TEST_CONSUMER_OUT",
                "exchange_type": "topic",
            },
        ],
        "FILE_STORAGE_URL": "",
        "STATIC_STORAGE_CACHE_DIR": None,
        "JOB_LOG_DIR": tmpdir,
        "CPUS": 1,
        "GPUS": [],
    }

@pytest.fixture
def main(config):
    fs = MockFileStorageClient()
    ss = MockFileStorageClient()
    m = Main(
        config=config,
        file_storage=fs,
        static_storage=ss
    )
    m.start(blocking=False)
    # Wait for setting up consumer
    time.sleep(5)
    yield m

    m.stop()


def test_main(mq_container, mq_base, scp_tar, main):

    # Exchange should already have been setup, but best practice is to always to it.
    mq_base.setup_exchange("TEST_CONSUMER_IN", "topic")
    mq_base.setup_exchange("TEST_CONSUMER_OUT", "topic")

    # mq_base must also make a queue and catch what is published from mq_sub
    q_out = mq_base.setup_queue_and_bind("TEST_CONSUMER_OUT", routing_key="success")

    # Generate the scp_context, which the fingerprinter should retrieve and process.

    model = Model(
        docker_kwargs={"image": "busybox",
                       "command": "sh -c 'cp /input/* /output/'"},
    )
    input_mount_mapping = {
        "src": main.file_storage.post(scp_tar)
    }
    pending_model_context = PendingModelContext(
        model=model,
        input_mount_mapping=input_mount_mapping
    )
    # Publish to the exchange and routing key that the mq_sub is listening to.
    mq_base.basic_publish(exchange="TEST_CONSUMER_IN",
                          routing_key="SCHEDULED_CPU",
                          body=pending_model_context.model_dump_json())
    #
    # Wait for the entry to be processed
    time.sleep(5)

    method_frame, header_frame, body = mq_base._channel.basic_get(q_out, auto_ack=True)
    assert method_frame
    print(method_frame, header_frame, body)
