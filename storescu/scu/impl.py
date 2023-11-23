import logging
import os

from pydicom import dcmread
from pydicom.errors import InvalidDicomError
from pynetdicom import AE, StoragePresentationContexts


def post_folder_to_dicom_node(scu_ip, scu_port, scu_ae_title, dicom_dir) -> bool:
    ae = AE()
    ae.requested_contexts = StoragePresentationContexts

    assoc = ae.associate(scu_ip, scu_port, ae_title=scu_ae_title)
    if assoc.is_established:
        # Use the C-STORE service to send the dataset
        # returns the response status as a pydicom Dataset
        logging.info(f'Posting {dicom_dir} to {scu_ae_title} on: {scu_ip}:{scu_port}')
        for fol, subs, files in os.walk(dicom_dir):
            for file in files:
                p = os.path.join(fol, file)
                try:
                    ds = dcmread(p)
                    status = assoc.send_c_store(ds)
                    # Check the status of the storage request
                    if status:
                        pass
                        # If the storage request succeeded this will be 0x0000
                        # logging.info('C-STORE request status: 0x{0:04x}'.format(status.Status))
                    else:
                        logging.info('Connection timed out, was aborted or received invalid response')

                except InvalidDicomError as e:
                    pass
                except Exception as e:
                    logging.error(str(e))
                    raise e

        # Release the association
        assoc.release()
        return True
    else:
        logging.error('Association rejected, aborted or never connected')
        return False