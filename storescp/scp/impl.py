import logging
import os
from typing import Dict

from DicomFlowLib.contexts import FileContext
from pynetdicom import AE, evt, StoragePresentationContexts, _config


from DicomFlowLib.contexts.scpcontext import SCPContext
from DicomFlowLib.mq import MQPub

LOG_FORMAT = ('%(levelname)s:%(asctime)s:%(message)s')

class SCP:
    def __init__(self,
                 mq: MQPub,
                 ae_title: str,
                 hostname: str,
                 port: int,
                 log_level: int = 10,
                 pynetdicom_log_level: str = "standard",
                 use_compression: bool = False
                 ):

        logging.basicConfig(level=log_level, format=LOG_FORMAT)
        _config.LOG_HANDLER_LEVEL = pynetdicom_log_level
        self.logger = logging.getLogger(__name__)

        self.mq = mq
        self.ae_title = ae_title
        self.hostname = hostname
        self.port = port
        self.ae = None
        self.use_compression = use_compression

        self.contexts = {}  # container for SCPContexts

    def __del__(self):
        if self.ae:
            self.ae.shutdown()

    def handle_store(self, event):
        """Handle EVT_C_STORE events."""
        # Association id unique to this transaction
        assoc_id = event.assoc.native_id
        if not assoc_id in self.contexts.keys():
            self.contexts[assoc_id]: Dict[str, SCPContext] = {}

        # Get data set from event
        ds = event.dataset

        # Add the File Meta Information
        ds.file_meta = event.file_meta

        pid = ds.PatientID
        sop_class_uid = ds.SOPClassUID
        series_instance_uid = ds.SeriesInstanceUID
        sop_instance_uid = ds.SOPInstanceUID

        #  Make association obj if it does not exist
        if not pid in self.contexts[assoc_id].keys():
            self.contexts[assoc_id][pid] = SCPContext()

        filepath = os.path.join(self.contexts[assoc_id][pid].path, pid, sop_class_uid, series_instance_uid, sop_instance_uid + ".dcm")
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        ds.save_as(filepath, write_like_original=False)

        # Add file metas so they can be shipped on
        self.contexts[assoc_id][pid].add_meta(ds.to_json_dict())

        # Return a 'Success' status
        return 0x0000

    def handle_release(self, event):
        assoc_id = event.assoc.native_id
        self.logger.info(f"Publishing from assoc_id: {assoc_id}")
        print(self.contexts)
        # For each PID send a seperate tar
        for pid, context in self.contexts[assoc_id].items():
            try:
                file = FileContext()
                file.generate_tar_from_path(context.path)
                file = self.mq.publish_file(file=file)
                context.file_queue = file.queue
                context.file_checksum = file.checksum
                self.mq.publish(context=context)
            except Exception as e:
                raise e
            finally:
                del self.contexts[assoc_id]  # Files are deleted when contexts are destructed

    def run_scp(self, blocking=True):
        handler = [
            (evt.EVT_C_STORE, self.handle_store),
            (evt.EVT_RELEASED, self.handle_release)
        ]

        try:
            self.logger.info(
                f"Starting SCP -- InferenceServerDicomNode: {self.hostname}:{str(self.port)} - {self.ae_title}")

            # Create and run
            self.ae = AE(ae_title=self.ae_title)
            self.ae.supported_contexts = StoragePresentationContexts
            self.ae.maximum_pdu_size = 0
            self.ae.start_server((self.hostname, self.port), block=blocking, evt_handlers=handler)

        except OSError as ose:
            self.logger.error(
                f'Full error: \r\n{ose} \r\n\r\n Cannot start Association Entity servers')
            raise ose

