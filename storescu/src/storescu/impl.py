import json
import logging
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


class STORESCU:
    def __init__(self, file_storage: FileStorageClient, pub_routing_key_success: str,
                 pub_routing_key_fail: str,
                 ae_title: str,
                 ae_port: int,
                 ae_hostname: str,
                 log_level: int = 20):
        self.pub_routing_key_success = pub_routing_key_success
        self.pub_routing_key_fail = pub_routing_key_fail

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        self.fs = file_storage
        self.ae_title = ae_title
        self.ae_port = ae_port
        self.ae_hostname = ae_hostname
    def mq_entrypoint(self, basic_deliver, body) -> Iterable[PublishContext]:

        fc = FlowContext(**json.loads(body.decode()))
        self.uid = fc.uid

        self.logger.info("SCU")

        self.logger.info("EXTRACTING FILE(S)")
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_tar = self.fs.get(fc.mount_mapping["dst"])
            with tarfile.TarFile.open(fileobj=output_tar, mode="r:*") as tf:
                tf.extractall(tmp_dir)

            self.logger.info("EXTRACTING FILE(S)")

            for port in fc.flow.return_to_sender_on_ports:
                self.logger.info(
                    f"POSTING TO SENDER: {fc.sender.ae_title} ON: {fc.sender.host}:{fc.sender.port}",
                )
                sender = fc.sender
                sender.port = port
                self.post_folder_to_dicom_node(dicom_dir=tmp_dir, destination=sender)


            for dest in fc.flow.destinations:
                self.logger.info(f"POSTING TO {dest.ae_title} ON: {dest.host}:{dest.port}")
                self.post_folder_to_dicom_node(dicom_dir=tmp_dir, destination=dest)

        self.logger.info("SCU")
        self.uid = None

        return [PublishContext(body=fc.model_dump_json().encode(), routing_key=self.pub_routing_key_success)]

    def post_folder_to_dicom_node(self, dicom_dir, destination: Destination) -> bool:
        ae = AE(ae_title=self.ae_title)
        ae.requested_contexts = StoragePresentationContexts

        assoc = ae.associate(destination.host, destination.port, ae_title=destination.ae_title,
                             bind_address=(self.ae_hostname, self.ae_port))
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
                            self.logger.debug('C-STORE request status: 0x{0:04x}'.format(status.Status))
                        else:
                            self.logger.info('Connection timed out, was aborted or received invalid response')
                    except InvalidDicomError as e:
                        pass
                    except Exception as e:
                        self.logger.error(f"{p}, {str(e)}")

            # Release the association
            self.logger.debug('Releasing Association')
            assoc.release()
            return True
        else:
            self.logger.error('Association rejected, aborted or never connected')
            return False
