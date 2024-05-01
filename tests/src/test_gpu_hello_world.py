import time

import pytest

from RadDeployLib.data_structures.flow import Model, Flow, Destination
from RadDeployLib.data_structures.service_contexts import FlowContext
from RadDeployLib.fp import generate_df_from_tar
from RadDeployLib.test_utils.fixtures import scp_tar, scp_tar_path
from .fixtures import *


@pytest.fixture
def ubuntu():
    return Model(
        docker_kwargs={
            "image": "ubuntu",
            "command": "bash -c 'nvidia-smi'"
        }
    )

def test_hello_world_gpu(ubuntu, scp_tar, mq_base_real, logger, fs_real):
    """
    This test injects a GPU-"hello-world" flow to the scheduler, and evaluates that it is successfully executed and
     published on scheduler success
    """
    uid = fs_real.post(scp_tar)
    df = generate_df_from_tar(scp_tar)
    flow_context = FlowContext(
        dataframe=df,
        src_uid=uid,
        flow=Flow(models=[ubuntu]),
        sender=Destination(host="test_service", port=1337, ae_title="TestService")
    )
    scheduler_success = mq_base_real.setup_queue("", auto_delete=True)
    mq_base_real.bind_queue(queue=scheduler_success,
                            exchange="scheduler",
                            routing_key="success")
    mq_base_real.bind_queue(queue=scheduler_success,
                            exchange="scheduler",
                            routing_key="fail")

    mq_base_real.basic_publish(exchange="fingerprinter", routing_key="success",
                               body=flow_context.model_dump_json().encode())

    t0 = time.time()
    while time.time() - t0 < 60:
        method_frame, header_frame, body = mq_base_real._channel.basic_get(scheduler_success, auto_ack=True)
        if method_frame:
            assert method_frame.routing_key == "success"
        else:
            time.sleep(1)
    else:
        raise Exception("Timed out waiting for messages")