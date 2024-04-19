import multiprocessing.pool
import os
import queue
import tarfile

import pytest
from scp import SCP

from DicomFlowLib.test_utils.mock_scu import post_folder_to_dicom_node
from DicomFlowLib.test_utils.fixtures import scan_dir


@pytest.fixture
def scp():
    scp = SCP(
        out_queue=queue.Queue(),
        ae_title="DicomFlow",
        hostname="localhost",
        port=10000,
        pynetdicom_log_level=20,
        log_level=10
    )
    scp.start(blocking=False)
    yield scp
    scp.stop()


def test_scp_single(scp, scan_dir):
    post_folder_to_dicom_node(0,
                              scan_dir,
                              ip=scp.hostname,
                              port=scp.port,
                              ae_title=scp.ae_title)

    scp_association = scp.out_queue.get()
    assert len(scp_association.dicom_files) == 2
    with tarfile.TarFile.open(fileobj=scp_association.as_tar()) as tf:
        for memb in tf.getmembers():
            assert memb.name in ["0.dcm", "1.dcm"]


def test_stress_scp(scp, scan_dir):
    # Dealing with many simultaneous posts
    t = multiprocessing.pool.ThreadPool(10)
    t.starmap(post_folder_to_dicom_node, [(i, scan_dir, scp.hostname, scp.port, scp.ae_title) for i in range(100)])
    t.close()
    t.join()
    assert scp.out_queue.qsize() == 100
    prev = scp.out_queue.get()
    while not scp.out_queue.empty:
        next = scp.out_queue.get()
        assert prev.model_dump_json() == next.model_dump_json()

    scp.stop()


if __name__ == "__main__":
    pytest.main()
