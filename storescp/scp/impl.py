import logging
import os
import tarfile
import tempfile
import traceback
from io import BytesIO
from queue import Queue
from typing import Dict

from pynetdicom import AE, evt, StoragePresentationContexts, _config
from python_logging_rabbitmq import RabbitMQHandler

from DicomFlowLib.data_structures.contexts import FlowContext, PublishContext
from DicomFlowLib.default_config import LOG_FORMAT
from DicomFlowLib.fs import FileStorage


class AssocContext:
    def __init__(self):
        self.context = FlowContext()
        self.file = BytesIO()
        self.tar = tarfile.TarFile.open(fileobj=self.file, mode="w:gz")

    def get_tar(self):
        return self.tar

    def get_file(self):
        self.file.seek(0)
        return self.file

    def get_file_size(self):
        try:
            self.file.seek(0, 2)
            return self.file.tell()
        finally:
            self.file.seek(0)

    def add_file_to_tar(self, path, file):
        info = tarfile.TarInfo(path)
        file.seek(0, 2)  # Move to end
        info.size = file.tell()
        file.seek(0)  # Reset pointer before read
        self.tar.addfile(info, file)
        file.close()

    def purge(self):
        if self.tar.open:
            self.tar.close()
        if not self.file.closed:
            self.file.close()

class SCP:
    def __init__(self,
                 publish_queue: Queue,
                 file_storage: FileStorage,
                 ae_title: str,
                 hostname: str,
                 port: int,
                 log_level: int,
                 mainstream_queue: str,
                 mq_logger: RabbitMQHandler | None = None,
                 pynetdicom_log_level: str = "standard"):
        self.fs = file_storage
        self.ae = None

        self.mainstream_queue = mainstream_queue

        logging.basicConfig(level=log_level, format=LOG_FORMAT)
        _config.LOG_HANDLER_LEVEL = pynetdicom_log_level
        self.LOGGER = logging.getLogger(__name__)
        if mq_logger:
            self.LOGGER.addHandler(mq_logger)

        self.publish_queue = publish_queue
        self.ae_title = ae_title
        self.hostname = hostname
        self.port = port

        self.assoc: Dict[AssocContext] = {}  # container for SCPContexts

    def __del__(self):
        if self.ae is not None:
            self.ae.shutdown()

    def handle_established(self, event):
        # Association id unique to this transaction
        # Set up all the things
        assoc_id = event.assoc.native_id
        self.assoc[assoc_id] = AssocContext()

        return 0x0000

    def handle_store(self, event):
        """Handle EVT_C_STORE events."""
        assoc_id = event.assoc.native_id

        # Get data set from event
        ds = event.dataset

        # Add the File Meta Information
        ds.file_meta = event.file_meta

        # Add file metas so they can be shipped on
        self.assoc[assoc_id].context.add_meta(ds.to_json_dict())

        # Save dcm file to tar
        path_in_tar = os.path.join("/", ds.PatientID, ds.SOPClassUID, ds.SeriesInstanceUID, ds.SOPInstanceUID + ".dcm")
        with tempfile.TemporaryFile() as file:
            ds.save_as(file, write_like_original=True)
            self.assoc[assoc_id].add_file_to_tar(path=path_in_tar, file=file)

        # Return a 'Success' status
        return 0x0000

    def publish_main_context(self, assoc_id):
        self.LOGGER.info(f"Queueing {assoc_id} up to be published")
        publish_context = PublishContext(
            routing_key=self.mainstream_queue,
            body=self.assoc[assoc_id].context.model_dump_json().encode()
        )
        self.publish_queue.put(publish_context, block=True)

    def publish_file_context(self, assoc_id):
        self.assoc[assoc_id].get_tar().close()
        return self.fs.put(self.assoc[assoc_id].get_file())

    def handle_release(self, event):
        assoc_id = event.assoc.native_id
        self.LOGGER.info(f"Publishing from assoc_id: {assoc_id}")
        uid = self.publish_file_context(assoc_id=assoc_id)
        try:
            self.assoc[assoc_id].context.input_file_uid = uid
            self.publish_main_context(assoc_id=assoc_id)
        except Exception as e:
            self.LOGGER.error(str(e))
        finally:
            self.assoc[assoc_id].purge()
            del self.assoc[assoc_id]
    def handle_echo(self, event):
        return 0x0000

    def run_scp(self, blocking=True):
        handler = [
            (evt.EVT_C_ECHO, self.handle_echo),
            (evt.EVT_ESTABLISHED, self.handle_established),
            (evt.EVT_C_STORE, self.handle_store),
            (evt.EVT_RELEASED, self.handle_release),
        ]

        try:
            self.LOGGER.info(
                f"Starting SCP -- InferenceServerDicomNode: {self.hostname}:{str(self.port)} - {self.ae_title}")

            # Create and run
            self.ae = AE(ae_title=self.ae_title)
            self.ae.supported_contexts = StoragePresentationContexts
            self.ae.maximum_pdu_size = 0
            self.ae.start_server((self.hostname, self.port), block=blocking, evt_handlers=handler)

        except OSError as ose:
            self.LOGGER.error(
                f'Full error: \r\n{ose} \r\n\r\n Cannot start Association Entity servers')
            raise ose
        except Exception as e:
            self.LOGGER.error(str(e))
            self.LOGGER.error(traceback.format_exc())
            raise e
