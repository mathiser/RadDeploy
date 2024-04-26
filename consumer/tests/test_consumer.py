import time

import pytest

from DicomFlowLib.data_structures.flow import Model
from DicomFlowLib.data_structures.service_contexts import JobContext
from DicomFlowLib.mq import PubModel, SubModel
from DicomFlowLib.test_utils.mock_classes import MockFileStorageClient
from consumer.src.consumer.impl import Consumer
from DicomFlowLib.test_utils.fixtures import mq_container, mq_base, scp_tar, scp_tar_path

@pytest.fixture
def config():
    return {
        # RabbitMQ
        "RABBIT_HOSTNAME": "localhost",
        "RABBIT_PORT": 5677,
        "LOG_LEVEL": 10,
        "LOG_FORMAT": "%(name)s ; %(levelname)s ; %(asctime)s ; %(name)s ; %(filename)s ; %(funcName)s ; %(lineno)s ; %(message)s",
        "LOG_PUB_MODELS": [
            {"exchange": "logs"},
        ],
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
    }
@pytest.fixture
def consumer(config, tmpdir):
    fs = MockFileStorageClient(log_level=10)
    ss = MockFileStorageClient()
    main = Consumer(
        rabbit_hostname=config["RABBIT_HOSTNAME"],
        rabbit_port=config["RABBIT_PORT"],
        log_dir=tmpdir,
        log_level=config["LOG_LEVEL"],
        log_pub_models=[PubModel(**pm) for pm in config["LOG_PUB_MODELS"]],
        log_format=config["LOG_FORMAT"],
        pub_models=[PubModel(**pm) for pm in config["PUB_MODELS"]],
        sub_models=[SubModel(**sm) for sm in config["CPU_SUB_MODELS"]],
        worker_type="CPU",
        worker_device_id="0",
        file_storage=fs,
        static_storage=ss,
        sub_prefetch_value=config["SUB_PREFETCH_COUNT"],
        sub_queue_kwargs=config["CPU_SUB_QUEUE_KWARGS"],
        job_log_dir=tmpdir
    )
    main.start()
    yield main
    main.stop()

def test_consumer(mq_container, mq_base, scp_tar, consumer):
    fs = consumer.file_storage

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
        "src": fs.post(scp_tar)
    }
    pending_job_context = JobContext(
        model=model,
        input_mount_mapping=input_mount_mapping
    )
    # Publish to the exchange and routing key that the mq_sub is listening to.
    mq_base.basic_publish(exchange="TEST_CONSUMER_IN",
                          routing_key="SCHEDULED_CPU",
                          body=pending_job_context.model_dump_json())
    #
    # Wait for the entry to be processed
    time.sleep(10)

    method_frame, header_frame, body = mq_base._channel.basic_get(q_out, auto_ack=True)
    assert method_frame

    print(method_frame, header_frame, body)
