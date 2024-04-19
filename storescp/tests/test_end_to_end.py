import queue
import time
from typing import Tuple

from DicomFlowLib.mq.mq_models import PublishContext
from DicomFlowLib.mq import MQPub, PubModel
from scp import SCP
from scp.models import SCPAssociation
from scp_release_handler.impl import SCPReleaseHandler
from DicomFlowLib.test_utils.fixtures import mq_container, fs, mq_base, scan_dir
from DicomFlowLib.test_utils.mock_scu import post_folder_to_dicom_node


def test_end_to_end(mq_container, fs, mq_base, scan_dir):
    exchange_name = "SCP_TEST"
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

    post_folder_to_dicom_node(0,
                              scan_dir,
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