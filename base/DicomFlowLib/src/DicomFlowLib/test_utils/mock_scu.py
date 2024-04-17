import logging
import os

import pydicom
from pydicom.errors import InvalidDicomError
from pynetdicom import StoragePresentationContexts, AE
from pydicom import misc

def post_folder_to_dicom_node(i, dicom_dir, ip, port, ae_title) -> bool:
    ae = AE(ae_title=ae_title)

    logger = logging.getLogger(__file__)
    logger.setLevel(20)

    ae.requested_contexts = StoragePresentationContexts
    assoc = ae.associate(ip, port, ae_title=ae_title)
    if assoc.is_established:
        # Use the C-STORE service to send the dataset
        # returns the response status as a pydicom Dataset
        for fol, subs, files in os.walk(dicom_dir):
            for file in files:
                p = os.path.join(fol, file)
                if not misc.is_dicom(file_path=str(p)):
                    continue
                try:
                    ds = pydicom.dcmread(p, force=True)
                    status = assoc.send_c_store(ds)
                    # Check the status of the storage request
                    if status:
                        # If the storage request succeeded this will be 0x0000
                        logger.debug('C-STORE request status: 0x{0:04x}'.format(status.Status))
                    else:
                        logger.debug('Connection timed out, was aborted or received invalid response')
                except InvalidDicomError as e:
                    logger.error(e)
                except Exception as e:
                    logger.error(f"{p}, {str(e)}")

        # Release the association
        assoc.release()
        return True
    else:
        return False
