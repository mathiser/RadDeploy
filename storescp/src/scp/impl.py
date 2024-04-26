import logging
import queue
import time
from io import BytesIO
from typing import List

from ip_filter import IPFilter
from pydicom.filewriter import write_file_meta_info
from pynetdicom import AE, evt, StoragePresentationContexts, VerificationPresentationContexts
from pynetdicom.events import Event


from .models import SCPAssociation, SCPAssociationManager
from .utils import generate_destination_from_event


class SCP:
    def __init__(self,
                 ae_title: str,
                 hostname: str,
                 port: int,
                 pynetdicom_log_level: int,
                 out_queue: queue.Queue[SCPAssociation],
                 blacklist_networks: List[str] | None = None,
                 whitelist_networks: List[str] | None = None,
                 maximum_pdu_size: int = 0,
                 log_level: int = 20):

        # Logger for the SCP
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)

        logging.getLogger("pynetdicom").setLevel(pynetdicom_log_level)

        # SCP setup
        self.ae = None
        self.ae_title = ae_title
        self.hostname = hostname
        self.port = port
        self.maximum_pdu_size = maximum_pdu_size
        self.ip_filter = IPFilter(whitelist=whitelist_networks,
                                  blacklist=blacklist_networks)

        # SCP release queue is where SCPAssociations are put to
        # be popped by someone else
        self.out_queue: queue.Queue[SCPAssociation] = out_queue

        # Container to keep track of incoming associations
        self.assoc_manager = SCPAssociationManager()

    def __del__(self):
        if self.ae is not None:
            self.ae.shutdown()

    def validate_sender(self, event: Event):
        """
        Called upon all associations have been established.
        Checks if connection is allowed according to black/whitelist
        :param event:
        :return:
        """
        if self.ip_filter.is_ip_allowed(ip=event.assoc.requestor.address):
            self.logger.debug(f"SCU {event.assoc.requestor.address} shall pass!")
            return 0x0000

        else:
            self.logger.error("SCU hostname is blacklisted - shall not pass")
            raise Exception("On blacklisted")

    def handle_store(self, event):
        """Handle EVT_C_STORE events."""
        self.logger.debug(f"Handling store")
        assoc_id = event.assoc.native_id
        scp_association = self.assoc_manager.get_scp_association(assoc_id)
        if not scp_association:
            destination = generate_destination_from_event(event)
            scp_association = self.assoc_manager.maybe_add_scp_association(SCPAssociation(assoc_id=assoc_id,
                                                                                          sender=destination))

        self.logger.debug(f"Writing DICOM file to SCP association")
        f = BytesIO()
        f.write(b'\x00' * 128)  # Write the preamble
        f.write(b'DICM')  # Write prefix
        write_file_meta_info(f, event.file_meta)  # Encode and write the File Meta Information
        f.write(event.request.DataSet.getvalue())  # Write the encoded dataset
        f.seek(0)
        scp_association.add_dicom_file(f)

        # Return a 'Success' status
        return 0x0000

    def handle_release(self, event: Event):
        self.maybe_release_storescp(event)
        return 0x0000

    def maybe_release_storescp(self, event):
        assoc_id = event.assoc.native_id

        # Return None or the SCPAssociation if it exists
        scp_association: SCPAssociation = self.assoc_manager.get_scp_association(assoc_id=assoc_id)
        if not scp_association:
            return
        else:
            try:
                self.logger.debug(f"HANDLE_RELEASE: {assoc_id}")
                self.out_queue.put(scp_association)
            except Exception as e:
                self.logger.error(str(e))
                raise e
            finally:
                self.assoc_manager.delete_scp_association(assoc_id)

    def handle_echo(self, event):
        self.logger.debug(f"Replying to ECHO")
        return 0x0000


    def stop(self, signalnum=None, stack_frame=None):
        self.logger.info("Stopping SCP")
        self.ae.shutdown()
        time.sleep(5)


    def start(self, blocking=True):
        handler = [
            (evt.EVT_C_ECHO, self.handle_echo),
            (evt.EVT_ESTABLISHED, self.validate_sender),
            (evt.EVT_C_STORE, self.handle_store),
            (evt.EVT_RELEASED, self.handle_release),
        ]

        try:
            self.logger.info(
                f"Starting SCP on host: {self.hostname}, port:{str(self.port)}, ae title: {self.ae_title}")

            # Create and run
            self.ae = AE(ae_title=self.ae_title)
            self.ae.supported_contexts = StoragePresentationContexts + VerificationPresentationContexts

            self.ae.maximum_pdu_size = self.maximum_pdu_size
            self.ae.start_server((self.hostname, self.port), block=blocking, evt_handlers=handler)
            self.logger.info(
                f"Starting SCP on host: {self.hostname}, port:{str(self.port)}, ae title: {self.ae_title}")
            return self
        except OSError as ose:
            self.logger.error(f'Cannot start Association Entity servers')
            raise ose
        except Exception as e:
            self.logger.error(str(e))
            raise e
