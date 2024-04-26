
import yaml
from main import Main

from DicomFlowLib.data_structures.flow import Model
from DicomFlowLib.data_structures.service_contexts import JobContext
from DicomFlowLib.test_utils.fixtures import *

@pytest.fixture
def config(tmpdir, mq_container):
    with open(os.path.join(os.path.dirname(__file__), "..", "conf.d", "00_default_config.yaml"), "r") as r:
        config = yaml.safe_load(r)

    config["RABBIT_HOSTNAME"] = "localhost"
    config["RABBIT_PORT"] = 5677
    config["LOG_DIR"] = tmpdir
    config["LOG_LEVEL"] = 10
    config["AE_HOSTNAME"] = "localhost"
    config["JOB_LOG_DIR"] = tmpdir
    config["FILE_STORAGE_URL"] = ""
    config["STATIC_STORAGE_CACHE_DIR"] = None

    return config

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
    mq_base.setup_exchange("scheduler", "topic")
    mq_base.setup_exchange("consumer", "topic")

    # mq_base must also make a queue and catch what is published from mq_sub
    q_out = mq_base.setup_queue_and_bind("consumer", routing_key="success")

    # Generate the scp_context, which the fingerprinter should retrieve and process.

    model = Model(
        docker_kwargs={"image": "busybox",
                       "command": "sh -c 'cp /input/* /output/'"},
    )
    input_mount_mapping = {
        "src": main.file_storage.post(scp_tar)
    }
    pending_job_context = JobContext(
        model=model,
        input_mount_mapping=input_mount_mapping,
        correlation_id=0,
        flow_id="asdf"
    )
    # Publish to the exchange and routing key that the mq_sub is listening to.
    mq_base.basic_publish(exchange="scheduler",
                          routing_key="cpu",
                          body=pending_job_context.model_dump_json())
    #
    # Wait for the entry to be processed
    time.sleep(5)

    method_frame, header_frame, body = mq_base._channel.basic_get(q_out, auto_ack=True)
    assert method_frame
    print(method_frame, header_frame, body)
