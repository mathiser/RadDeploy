import copy

from DicomFlowLib.data_structures.contexts import PublishContext, SCPContext
from DicomFlowLib.data_structures.flow import Destination
from DicomFlowLib.test_utils.fixtures import *
from DicomFlowLib.mq import PubModel, MQSub, SubModel
from fingerprinting import Fingerprinter


@pytest.fixture
def fp(fs, flow_dir):
    return Fingerprinter(file_storage=fs,
                         flow_directory=flow_dir,
                         log_level=10)


def test_end_to_end(mq_container, fp, mq_base, flow_dir, scp_tar):
    # Test exchange
    exchange_name = "FINGERPRINTER_TEST"
    routing_key_in = "pending"
    routing_key_out = "success"

    # Set up a MQSub, which uses fp.mq_entrypoint as work_function
    mq_sub = MQSub(rabbit_hostname=mq_base._hostname,
                   rabbit_port=mq_base._port,
                   sub_models=[SubModel(exchange=exchange_name,
                                        routing_keys=[routing_key_in])],
                   pub_models=[PubModel(exchange=exchange_name)],
                   work_function=fp.mq_entrypoint,
                   sub_prefetch_value=1,
                   sub_queue_kwargs={"auto_delete": True},
                   log_level=10)
    mq_sub.start()

    # Post a tar file to the filestorage to mimic something coming from the SCP
    src_uid = fp.fs.post(scp_tar)

    # Exchange should already have been setup, but best practice is to always to it.
    mq_base.setup_exchange(exchange_name, "topic")

    # mq_base must also make a queue and catch what is published from mq_sub
    q_out = mq_base.setup_queue_and_bind(exchange_name, routing_key_out)

    # Generate the scp_context, which the fingerprinter should retrieve and process.
    scp_context = SCPContext(src_uid=src_uid, sender=Destination(host="localhost", port=1234, ae_title="test"))

    res = fp.run_fingerprinting(scp_context=scp_context, tar_file=scp_tar)
    for r in res:
        assert r.model_dump_json()

    # Publish to the exchange and routing key that the mq_sub is listening to.
    mq_base.basic_publish(exchange=exchange_name,
                          routing_key=routing_key_in,
                          body=scp_context.model_dump_json())

    # Wait for the entry to be processed
    time.sleep(1)

    # Check that a flow context is published.
    method_frame, header_frame, body = mq_base._channel.basic_get(q_out, auto_ack=True)
    assert method_frame

    mq_sub.stop()
    mq_sub.join()
