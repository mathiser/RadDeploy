import json
import os
import tarfile
import tempfile

import pydicom
from pydicom.errors import InvalidDicomError
from pynetdicom import AE, StoragePresentationContexts

from DicomFlowLib.data_structures.contexts import FlowContext
from DicomFlowLib.data_structures.flow import Destination
from DicomFlowLib.fs import FileStorage
from DicomFlowLib.log import CollectiveLogger
from DicomFlowLib.mq import MQBase


class SCU:
    def __init__(self,
                 file_storage: FileStorage,
                 logger: CollectiveLogger,
                 pub_exchange: str,
                 pub_routing_key: str,
                 pub_routing_key_as_queue: bool,
                 pub_exchange_type: str):
                 
        self.logger = logger
        self.fs = file_storage
        self.pub_exchange_type = pub_exchange_type
        self.pub_routing_key = pub_routing_key
        self.pub_routing_key_as_queue = pub_routing_key_as_queue
        self.pub_exchange = pub_exchange
        self.pub_declared = False

    def maybe_declare_exchange_and_queue(self, mq):
        if not self.pub_declared:
            mq.setup_exchange_callback(exchange=self.pub_exchange, exchange_type=self.pub_exchange_type)
            if self.pub_routing_key_as_queue:
                mq.setup_queue_and_bind_callback(exchange=self.pub_exchange,
                                                 routing_key=self.pub_routing_key,
                                                 routing_key_as_queue=self.pub_routing_key_as_queue)
            self.pub_declared = True
            return True
        else:
            return False

    def mq_entrypoint(self, connection, channel, basic_deliver, properties, body):

        context = FlowContext(**json.loads(body.decode()))
        uid = context.uid

        self.logger.info("SCU", uid=uid, finished=False)

        self.logger.info("EXTRACTING FILE(S)", uid=uid, finished=False)
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_tar = self.fs.get(context.output_file_uid)
            with tarfile.TarFile.open(fileobj=output_tar, mode="r:*") as tf:
                tf.extractall(tmp_dir)

            self.logger.info("EXTRACTING FILE(S)", uid=uid, finished=True)

            if context.flow.return_to_sender:
                self.logger.info(f"POSTING TO SENDER: {context.sender.ae_title} ON: {context.sender.host}:{context.sender.port}", uid=uid, finished=False)
                self.post_folder_to_dicom_node(dicom_dir=tmp_dir, destination=context.sender)
                self.logger.info(f"POSTING TO SENDER: {context.sender.ae_title} ON: {context.sender.host}:{context.sender.port}", uid=uid, finished=True)


            for dest in context.flow.destinations:
                self.logger.info(f"POSTING TO {dest.ae_title} ON: {dest.host}:{dest.port}", uid=uid, finished=False)
                self.post_folder_to_dicom_node(dicom_dir=tmp_dir, destination=dest)
                self.logger.info(f"POSTING TO {dest.ae_title} ON: {dest.host}:{dest.port}", uid=uid, finished=True)

        mq = MQBase(logger=self.logger, close_conn_on_exit=False).connect_with(connection=connection, channel=channel)
        self.maybe_declare_exchange_and_queue(mq)
        mq.basic_publish_callback(exchange=self.pub_exchange,
                                  routing_key=self.pub_routing_key,
                                  body=context.model_dump_json().encode())
        self.logger.info("SCU", uid=uid, finished=True)

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
                            self.logger.debug('C-STORE request status: 0x{0:04x}'.format(status.Status))
                        else:
                            self.logger.info('Connection timed out, was aborted or received invalid response')
                    except InvalidDicomError as e:
                        self.logger.error(str(e), uid=p)
                    except Exception as e:
                        self.logger.error(str(e), uid=p)

            # Release the association
            assoc.release()
            return True
        else:
            self.logger.error('Association rejected, aborted or never connected')
            return False