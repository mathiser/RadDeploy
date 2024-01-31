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
                 pub_routing_key_fail: str
                 ):

        self.pub_routing_key_success = pub_routing_key_success
        self.pub_routing_key_fail = pub_routing_key_fail

        self.logger = logger
        self.fs = file_storage

    def mq_entrypoint(self, basic_deliver, body) -> Iterable[PublishContext]:

        context = FlowContext(**json.loads(body.decode()))
        self.uid = context.flow_instance_uid

        self.logger.info("SCU", uid=self.uid, finished=False)

        self.logger.info("EXTRACTING FILE(S)", uid=self.uid, finished=False)
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_tar = self.fs.get(context.output_file_uid)
            with tarfile.TarFile.open(fileobj=output_tar, mode="r:*") as tf:
                tf.extractall(tmp_dir)

            self.logger.info("EXTRACTING FILE(S)", uid=self.uid, finished=True)

            for port in context.flow.return_to_sender_on_ports:
                self.logger.info(
                    f"POSTING TO SENDER: {context.sender.ae_title} ON: {context.sender.host}:{context.sender.port}",
                    uid=self.uid, finished=False)
                sender = context.sender
                sender.port = port
                self.post_folder_to_dicom_node(dicom_dir=tmp_dir, destination=sender)
                self.logger.info(
                    f"POSTING TO SENDER: {context.sender.ae_title} ON: {context.sender.host}:{context.sender.port}",
                    uid=self.uid, finished=True)

            for dest in context.flow.destinations:
                self.logger.info(f"POSTING TO {dest.ae_title} ON: {dest.host}:{dest.port}", uid=self.uid,finished=False)
                self.post_folder_to_dicom_node(dicom_dir=tmp_dir, destination=dest)
                self.logger.info(f"POSTING TO {dest.ae_title} ON: {dest.host}:{dest.port}", uid=self.uid, finished=True)


        self.logger.info("SCU", uid=self.uid, finished=True)
        self.uid = None

        return [PublishContext(body=context.model_dump_json().encode(), routing_key=self.pub_routing_key_success)]


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
