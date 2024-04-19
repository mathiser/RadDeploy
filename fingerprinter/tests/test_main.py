from DicomFlowLib.data_structures.service_contexts import SCPContext
from DicomFlowLib.data_structures.flow import Destination
from DicomFlowLib.test_utils.fixtures import *
from main import Main

@pytest.fixture
def config(tmp_path, flow_dir):
    return {
        # RabbitMQ
        "RABBIT_HOSTNAME": "localhost",
        "RABBIT_PORT": 5677,
        "LOG_DIR": tmp_path,
        "LOG_LEVEL": 10,
        "LOG_FORMAT": "%(name)s ; %(levelname)s ; %(asctime)s ; %(name)s ; %(filename)s ; %(funcName)s ; %(lineno)s ; %(message)s",
        "LOG_PUB_MODELS": [
            {"exchange": "logs"},
        ],
        "FLOW_DIRECTORY": flow_dir,
        "SUB_QUEUE_KWARGS": {
            "queue": "",
            "passive": False,
            "durable": False,
            "exclusive": False,
            "auto_delete": True,
        },
        "SUB_PREFETCH_COUNT": 1,
        "SUB_MODELS": [
            {
                "exchange": "FINGERPRINTER_IN",
                "exchange_type": "topic",
                "routing_keys": ["PENDING"],
                "routing_key_fetch_echo": None,
            },
        ],
        "PUB_MODELS": [
            {
                "exchange": "FINGERPRINTER_OUT",
                "exchange_type": "topic",
            },
        ],
        "FILE_STORAGE_URL": ""
    }


def test_end_to_end(mq_container, mq_base, scp_tar, config):
    main = Main(config, MockFileStorageClient)
    main.start()


    # Post a tar file to the filestorage to mimic something coming from the SCP
    src_uid = main.fs.post(scp_tar)
    scp_tar.seek(0)

    # Exchange should already have been setup, but best practice is to always to it.
    mq_base.setup_exchange("FINGERPRINTER_IN", "topic")
    mq_base.setup_exchange("FINGERPRINTER_OUT", "topic")

    # mq_base must also make a queue and catch what is published from mq_sub
    q_out = mq_base.setup_queue_and_bind("FINGERPRINTER_OUT", routing_key="success")

    # Generate the scp_context, which the fingerprinter should retrieve and process.
    scp_context = SCPContext(src_uid=src_uid, sender=Destination(host="localhost", port=1234, ae_title="test"))

    # Publish to the exchange and routing key that the mq_sub is listening to.
    q_in = mq_base.setup_queue_and_bind("FINGERPRINTER_IN", routing_key="PENDING")
    mq_base.basic_publish(exchange="FINGERPRINTER_IN",
                          routing_key="PENDING",
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