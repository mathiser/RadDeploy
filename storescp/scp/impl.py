import logging
import traceback
from queue import Queue

from pynetdicom import AE, evt, StoragePresentationContexts, _config

from DicomFlowLib.contexts import FileContext, BaseContext
from DicomFlowLib.contexts.scp import SCPContext
from DicomFlowLib.default_config import LOG_FORMAT


class SCP:
    def __init__(self,
                 scheduled_contexts: Queue[BaseContext],
                 ae_title: str,
                 hostname: str,
                 port: int,
                 pub_exchange: str,
                 pub_routing_key: str,
                 log_level: int,
                 pynetdicom_log_level: str = "standard"):

        logging.basicConfig(level=log_level, format=LOG_FORMAT)
        _config.LOG_HANDLER_LEVEL = pynetdicom_log_level
        self.logger = logging.getLogger(__name__)

        self.scheduled_contexts = scheduled_contexts
        self.pub_routing_key = pub_routing_key
        self.pub_exchange = pub_exchange

        self.ae_title = ae_title
        self.hostname = hostname
        self.port = port

        self.ae = None

        self.contexts = {}  # container for SCPContexts

    def __del__(self):
        if self.ae:
            self.ae.shutdown()

    def handle_store(self, event):
        """Handle EVT_C_STORE events."""
        # Association id unique to this transaction
        assoc_id = event.assoc.native_id
        if not assoc_id in self.contexts.keys():
            print("Putting context")
            self.contexts[assoc_id] = SCPContext(routing_key=self.pub_routing_key,
                                                 exchange=self.pub_exchange,
                                                 routing_key_as_queue=True,  # Specifies the work queue
                                                 file_exchange=self.pub_exchange)  # For now use same exchange as mainstream
            print(self.contexts[assoc_id])
        # Get data set from event
        ds = event.dataset

        # Add the File Meta Information
        ds.file_meta = event.file_meta

        # Add file metas so they can be shipped on
        self.contexts[assoc_id].add_meta(ds.to_json_dict())
        #self.scheduled_contexts.put(
        #    FileContext(
        #        exchange=self.pub_exchange,
        #        routing_key=self.contexts[assoc_id].file_routing_key,
        #        file=ds.to_json_dict(),
        #    ), block=True)

        # Return a 'Success' status
        return 0x0000

    def handle_release(self, event):
        assoc_id = event.assoc.native_id
        self.logger.info(f"Publishing from assoc_id: {assoc_id}")

        try:
            self.logger.info(f"Received dicom files. Publishing")
            self.scheduled_contexts.put(self.contexts[assoc_id], block=True)
        except Exception as e:
            print(traceback.format_exc())
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
        except Exception as e:
            self.logger.error(str(e))
            self.logger.error(traceback.format_exc())
            raise e
