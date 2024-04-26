import json
import logging
import threading
import uuid

import yaml

from DicomFlowLib.data_structures.service_contexts.models import JobContext
from DicomFlowLib.test_utils.fixtures import *
from main import Main

from db import DBJob


@pytest.fixture
def flow_exchange():
    return "fingerprinter"


@pytest.fixture
def consumer_exchange():
    return "consumer"


@pytest.fixture
def scheduler_exchange():
    return "scheduler"


@pytest.fixture
def config(tmpdir, mq_container, flow_exchange, consumer_exchange, scheduler_exchange):
    with open(os.path.join(os.path.dirname(__file__), "..", "conf.d", "00_default_config.yaml"), "r") as r:
        config = yaml.safe_load(r)

    config["RABBIT_HOSTNAME"] = "localhost"
    config["RABBIT_PORT"] = 5677
    config["LOG_DIR"] = tmpdir
    config["LOG_LEVEL"] = 10
    config["SCHEDULER_DATABASE_PATH"] = os.path.join(tmpdir, "db.db")

    return config


@pytest.fixture
def main(config):
    main = Main(config)
    main.start(blocking=False)
    yield main
    main.stop()


class MockConsumer:
    def __init__(self, mq_base, scheduler_exchange, scheduler_routing_key_out, consumer_exchange):
        super().__init__()
        self.logger = logging.getLogger("MockConsumer")
        self.logger.setLevel(10)
        self.running = False
        self.mq_base = mq_base
        self.consumer_exchange = consumer_exchange

        mq_base.setup_exchange(scheduler_exchange, "topic")
        mq_base.setup_exchange(consumer_exchange, "topic")

        self.q = mq_base.setup_queue_and_bind(scheduler_exchange,
                                              routing_key=scheduler_routing_key_out,
                                              routing_key_as_queue=True)
        self.mq_base.bind_queue(exchange=scheduler_exchange, queue=self.q, routing_key="cpu")

    def consume(self):
        self.logger.info(f"Retrieving from queue {self.q}")
        method_frame, header_frame, body = self.mq_base._channel.basic_get(self.q, auto_ack=True)
        if method_frame:
            job_context = JobContext(**json.loads(body))
            self.logger.info(f"consumer received a job: {job_context.model_dump_json()}")

            # Mock output mounts
            for k in job_context.model.output_mount_keys:
                job_context.output_mount_mapping[k] = str(uuid.uuid4())

            self.logger.debug(
                f"MockConsumer publishes on {self.consumer_exchange} generated this job: {job_context.model_dump_json()}")
            self.mq_base.basic_publish(exchange=self.consumer_exchange,
                                       routing_key="success",
                                       body=job_context.model_dump_json())
            return job_context


@pytest.fixture
def cpu_consumer(mq_base,
                 scheduler_exchange,
                 consumer_exchange):
    return MockConsumer(mq_base=mq_base,
                        consumer_exchange=consumer_exchange,
                        scheduler_exchange=scheduler_exchange,
                        scheduler_routing_key_out="cpu")


@pytest.fixture
def gpu_consumer(mq_base,
                 scheduler_exchange,
                 consumer_exchange):
    return MockConsumer(mq_base=mq_base,
                        consumer_exchange=consumer_exchange,
                        scheduler_exchange=scheduler_exchange,
                        scheduler_routing_key_out="gpu")


def test_main_flow_to_job_queue(config, main, mq_base, scheduler_exchange, flow_exchange,
                                consumer_exchange,
                                dag_flow_context,
                                cpu_consumer,
                                gpu_consumer):
    print(main.mq.pub_models)
    # Exchange should already have been setup, but best practice is to always to it.
    mq_base.setup_exchange(scheduler_exchange, "topic")
    mq_base.setup_exchange(flow_exchange, "topic")
    mq_base.setup_exchange(consumer_exchange, "topic")

    scheulder_success_q = mq_base.setup_queue_and_bind(scheduler_exchange, routing_key="success")

    # Check that flows are retrieved correctly
    mq_base.basic_publish(exchange=flow_exchange,
                          routing_key="success",
                          body=dag_flow_context.model_dump_json())

    # Let all jobs be consumed
    time.sleep(2)

    jobs = main.db.get_all(DBJob)
    print(jobs)
    assert jobs[0].is_runnable()
    assert not jobs[1].is_runnable()
    assert not jobs[2].is_runnable()
    assert not jobs[3].is_runnable()

    while True:
        time.sleep(1)
        jc_cpu = cpu_consumer.consume()
        jc_gpu = gpu_consumer.consume()
        if not jc_cpu and not jc_gpu:
            break

    jobs = main.db.get_all(DBJob)
    for j in jobs:
        assert j.is_finished()

    time.sleep(1)
    method_frame, header_frame, body = mq_base._channel.basic_get(scheulder_success_q, auto_ack=True)
    print(method_frame)
    assert method_frame
