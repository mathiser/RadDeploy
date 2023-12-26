import json
import logging
import os
import tarfile
import tempfile

import pydicom
from pydicom.errors import InvalidDicomError
from pynetdicom import AE, StoragePresentationContexts
from python_logging_rabbitmq import RabbitMQHandler

from DicomFlowLib.data_structures.contexts import FlowContext
from DicomFlowLib.data_structures.flow import Destination
from DicomFlowLib.default_config import LOG_FORMAT
from DicomFlowLib.fs import FileStorage


class SCU:
    def __init__(self,
                 file_storage: FileStorage,
                 mq_logger: RabbitMQHandler | None = None,
                 
                 log_level: int = 10):
        logging.basicConfig(level=log_level, format=LOG_FORMAT)
        self.LOGGER = logging.getLogger(__name__)
        if mq_logger:
            self.LOGGER.addHandler(mq_logger)
        
        self.fs = file_storage
        
    def mq_entrypoint(self, connection, channel, basic_deliver, properties, body):
        context = FlowContext(**json.loads(body.decode()))
        self.LOGGER.info(str(context))
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_tar = self.fs.get(context.output_file_uid)
            with tarfile.TarFile.open(fileobj=output_tar, mode="r:*") as tf:
                tf.extractall(tmp_dir)
            
            for dest in context.flow.destinations:
                self.post_folder_to_dicom_node(dicom_dir=tmp_dir, destination=dest)

    def post_folder_to_dicom_node(self, dicom_dir, destination: Destination) -> bool:
        ae = AE()
        ae.requested_contexts = StoragePresentationContexts

        assoc = ae.associate(destination.host, destination.port, ae_title=destination.ae_title)
        if assoc.is_established:
            # Use the C-STORE service to send the dataset
            # returns the response status as a pydicom Dataset
            self.LOGGER.info(f'Posting {dicom_dir} to {destination.ae_title} on: {destination.host}:{destination.port}')
            for fol, subs, files in os.walk(dicom_dir):
                for file in files:
                    p = os.path.join(fol, file)
                    try:
                        ds = pydicom.dcmread(p, force=True)
                        status = assoc.send_c_store(ds)
                        # Check the status of the storage request
                        if status:
                            # If the storage request succeeded this will be 0x0000
                            self.LOGGER.debug('C-STORE request status: 0x{0:04x}'.format(status.Status))
                        else:
                            self.LOGGER.info('Connection timed out, was aborted or received invalid response')

                    except InvalidDicomError as e:
                        self.LOGGER.error(str(e))
                    except Exception as e:
                        self.LOGGER.error(str(e))
                        raise e

            # Release the association
            assoc.release()
            return True
        else:
            self.LOGGER.error('Association rejected, aborted or never connected')
            return False