import yaml
from main import Main

from RadDeployLib.test_utils.fixtures import *


@pytest.fixture
def config(tmpdir, mq_container, flow_dir):
    with open(os.path.join(os.path.dirname(__file__), "..", "conf.d", "00_default_config.yaml"), "r") as r:
        config = yaml.safe_load(r)

    config["RABBIT_HOSTNAME"] = "localhost"
    config["RABBIT_PORT"] = 5677
    config["LOG_DIR"] = tmpdir
    config["LOG_LEVEL"] = 10
    config["AE_HOSTNAME"] = "localhost"
    config["FLOW_DIRECTORY"] = flow_dir
    config["FILE_STORAGE_URL"] = ""

    return config

def test_end_to_end(mq_container, mq_base, scp_tar, config):
    main = Main(config, MockFileStorageClient)
    main.start()


    # Post a tar file to the filestorage to mimic something coming from the SCP
    src_uid = main.fs.post(scp_tar)
    scp_tar.seek(0)

    # Exchange should already have been setup, but best practice is to always to it.
    mq_base.setup_exchange("storescp", "topic")
    mq_base.setup_exchange("fingerprinter", "topic")

    # mq_base must also make a queue and catch what is published from mq_sub
    q_out = mq_base.setup_queue_and_bind("fingerprinter", routing_key="success")

    # Generate the scp_context, which the fingerprinter should retrieve and process.
    scp_context = SCPContext(src_uid=src_uid, sender=Destination(host="localhost", port=1234, ae_title="test"))

    # Publish to the exchange and routing key that the mq_sub is listening to.
    q_in = mq_base.setup_queue_and_bind("storescp", routing_key="success")
    mq_base.basic_publish(exchange="storescp",
                          routing_key="success",
                          body=scp_context.model_dump_json())
    #
    # Wait for the entry to be processed
    time.sleep(5)

    # Check that a seperate queue can in fact receive the data
    method_frame, header_frame, body = mq_base._channel.basic_get(q_in, auto_ack=True)
    print(method_frame, header_frame, body)

    method_frame, header_frame, body = mq_base._channel.basic_get(q_out, auto_ack=True)
    print(method_frame, header_frame, body)


    # Check that a flow context is published.
    method_frame, header_frame, body = mq_base._channel.basic_get(q_out, auto_ack=True)
    assert method_frame
    print(method_frame, header_frame, body)

    main.stop()