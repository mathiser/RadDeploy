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

def post(i, ip, port, ae_title):
    post_folder_to_dicom_node(dicom_dir=os.path.join(os.path.dirname(__file__), "test_data/scans"),
                              ip=ip,
                              port=port,
                              ae_title=ae_title)

def test_scp_single(scp):
    post(0,
         ip=scp.hostname,
         port=scp.port,
         ae_title=scp.ae_title)
    scp_association = scp.out_queue.get()
    assert len(scp_association.dicom_files) == 2
    with tarfile.TarFile.open(fileobj=scp_association.as_tar()) as tf:
        for memb in tf.getmembers():
            assert memb.name in ["0.dcm", "1.dcm"]

def test_stress_scp(scp):
    # Dealing with many simultaneous posts
    t = multiprocessing.pool.ThreadPool(10)
    t.starmap(post, [(i, scp.hostname, scp.port, scp.ae_title) for i in range(100)])
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
