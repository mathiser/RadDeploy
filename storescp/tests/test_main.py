import os
import tarfile
import tempfile
import time

import pytest
from main import Main

from DicomFlowLib.fs.client.interface import FileStorageClientInterface
from DicomFlowLib.test_utils.fixtures import mq_container, mq_base
from DicomFlowLib.test_utils.mock_scu import post_folder_to_dicom_node
from DicomFlowLib.test_utils.fixtures import scan_dir, scp_tar_path


@pytest.fixture
def config(tmp_path):
    return {
        "RABBIT_HOSTNAME": "localhost",
        "RABBIT_PORT": 5677,
        "LOG_DIR": tmp_path,
        "LOG_LEVEL": 10,
        "LOG_FORMAT": "%(name)s ; %(levelname)s ; %(asctime)s ; %(name)s ; %(filename)s ; %(funcName)s ; %(lineno)s ; %(message)s",
        "LOG_PUB_MODELS": [
            {"exchange": "logs"},
        ],
        "FILE_STORAGE_URL": "",
        "AE_TITLE": "DICOMFLOW",
        "AE_HOSTNAME": "localhost",
        "AE_PORT": 10101,
        "AE_BLACKLISTED_IP_NETWORKS": None,
        "AE_WHITELISTED_IP_NETWORKS": None,
        "PYNETDICOM_LOG_LEVEL": 10,
        "PUB_MODELS": [
            {
                "exchange": "SCP_OUT",
                "exchange_type": "topic",
            },
        ]
    }


def test_main(mq_container, config, mq_base, scan_dir, scp_tar_path):
    m = Main(config, FileStorageClientInterface)
    m.start(blocking=False)
    exchange_name = config["PUB_MODELS"][0]["exchange"]
    mq_base.setup_exchange(exchange_name, "topic")
    q_out = mq_base.setup_queue_and_bind(exchange_name, "#")
    os.system(f"python -m pynetdicom storescu -r -v {config["AE_HOSTNAME"]} {config["AE_PORT"]} {scan_dir}")

    time.sleep(1)

    method_frame, header_frame, body = mq_base._channel.basic_get(q_out, auto_ack=True)
    assert method_frame

    m.stop()
