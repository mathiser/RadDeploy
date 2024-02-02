import json
import os
import tarfile
import tempfile
from typing import Iterable

import pydicom
from pydicom.errors import InvalidDicomError
from pynetdicom import AE, StoragePresentationContexts

from DicomFlowLib.data_structures.contexts import FlowContext, PublishContext
from DicomFlowLib.data_structures.flow import Destination
from DicomFlowLib.fs import FileStorageClient
from DicomFlowLib.log import CollectiveLogger


class SCU:
    def __init__(self,
                 file_storage: FileStorageClient,
                 logger: CollectiveLogger,
                 pub_routing_key_success: str,
                 pub_routing_key_fail: str):

        self.pub_routing_key_success = pub_routing_key_success
        self.pub_routing_key_fail = pub_routing_key_fail

        self.logger = logger
        self.fs = file_storage
        self.uid = None

    def post_folder_to_dicom_node(self, dicom_dir, destination: Destination) -> bool:
        ae = AE()
        ae.requested_contexts = StoragePresentationContexts

        assoc = ae.associate(destination.host, destination.port, ae_title=destination.ae_title)
        if assoc.is_established:
            # Use the C-STORE service to send the dataset
            # returns the response status as a pydicom Dataset
            for fol, subs, files in os.walk(dicom_dir):
                for file in files:
                    p = os.path.join(fol, file)
                    try:
                        ds = pydicom.dcmread(p, force=True)
                        status = assoc.send_c_store(ds)
                        # Check the status of the storage request
                        if status:
                            # If the storage request succeeded this will be 0x0000
                            self.logger.debug('C-STORE request status: 0x{0:04x}'.format(status.Status), uid=self.uid)
                        else:
                            self.logger.info('Connection timed out, was aborted or received invalid response',
                                             uid=self.uid)

                    except InvalidDicomError as e:
                        pass
                    except Exception as e:
                        self.logger.error(str(e), uid=p)

            # Release the association
            assoc.release()
            return True
        else:
            self.logger.error('Association rejected, aborted or never connected')
            return False
