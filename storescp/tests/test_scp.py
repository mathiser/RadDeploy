import logging
import multiprocessing.pool
import os
import queue
import tarfile

import pydicom
import pytest
from pydicom.errors import InvalidDicomError
from pynetdicom import StoragePresentationContexts, AE
from scp import SCP


def post_folder_to_dicom_node(dicom_dir, ip, port, ae_title) -> bool:
    ae = AE(ae_title="DicomFlow")

    ae.requested_contexts = StoragePresentationContexts
    assoc = ae.associate(ip, port, ae_title=ae_title)
    if assoc.is_established:
        # Use the C-STORE service to send the dataset
        # returns the response status as a pydicom Dataset
        for fol, subs, files in os.walk(dicom_dir):
            print("#############################", files)

            for file in files:
                p = os.path.join(fol, file)
                try:
                    ds = pydicom.dcmread(p, force=True)
                    status = assoc.send_c_store(ds)
                    # Check the status of the storage request
                    if status:
                        # If the storage request succeeded this will be 0x0000
                        print('C-STORE request status: 0x{0:04x}'.format(status.Status))
                    else:
                        print('Connection timed out, was aborted or received invalid response')
                except InvalidDicomError as e:
                    pass
                except Exception as e:
                    print(f"{p}, {str(e)}")

        # Release the association
        assoc.release()
        return True
    else:
        return False


@pytest.fixture
def scp():
    return SCP(
        out_queue=queue.Queue(),
        ae_title="DicomFlow",
        hostname="localhost",
        port=10000,
        pynetdicom_log_level=20,
        log_level=10
    )


def test_scp(scp):
    scp.start(blocking=False)

    def post(i):
        post_folder_to_dicom_node(dicom_dir=os.path.join(os.path.dirname(__file__), "test_data/scans"),
                                  ip=scp.hostname,
                                  port=scp.port,
                                  ae_title=scp.ae_title)

    post(0)
    scp_association = scp.out_queue.get()
    assert len(scp_association.dicom_files) == 2
    with tarfile.TarFile.open(fileobj=scp_association.as_tar()) as tf:
        for memb in tf.getmembers():
            assert memb.name in ["0.dcm", "1.dcm"]

    # Dealing with many simultaneous posts
    t = multiprocessing.pool.ThreadPool(10)
    t.map(post, range(100))
    t.close()
    t.join()
    assert scp.out_queue.qsize() == 100
    scp.stop()


if __name__ == "__main__":
    pytest.main()
