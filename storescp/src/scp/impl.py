import logging
import os
import tarfile
import tempfile
from io import BytesIO
from typing import Dict, List

from pydicom.filewriter import write_file_meta_info
from pynetdicom import AE, evt, StoragePresentationContexts, _config, VerificationPresentationContexts
from pynetdicom.events import Event
from DicomFlowLib.data_structures.contexts import SCPContext, PublishContext
from DicomFlowLib.mq import PubModel
from DicomFlowLib.data_structures.flow import Destination
from DicomFlowLib.fs import FileStorageClient
from DicomFlowLib.mq import MQPub


class AssocContext:
    def __init__(self):
        self.file = BytesIO()
        self.tar = tarfile.TarFile.open(fileobj=self.file, mode="w")

        self.flow_context = SCPContext()

    def __del__(self):
        try:
            self.tar.close()
        finally:
            if not self.file.closed:
                self.file.close()

    def add_file_to_tar(self, path, file):
        file.seek(0, 2)
        info = tarfile.TarInfo(name=path)
        info.size = file.tell()
        file.seek(0)
        self.tar.addfile(info, file)
        return info.name

class SCP:
    def __init__(self, file_storage: FileStorageClient,
                 ae_title: str,
                 hostname: str,
                 port: int,
                 pub_models: List[PubModel],
                 pynetdicom_log_level: int,
                 tar_subdir: List[str],
                 routing_key_success: str,
                 routing_key_fail: str,
                 mq_pub: MQPub,
                 blacklisted_hosts: List[str] | None = None,
                 whitelisted_hosts: List[str] | None = None,
                 maximum_pdu_size: int = 0,
                 log_level: int = 20):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)

        logging.getLogger("pynetdicom").setLevel(pynetdicom_log_level)

        self.maximum_pdu_size = maximum_pdu_size
        self.fs = file_storage
        self.ae = None
        self.mq_pub = mq_pub
        self.tar_subdir = tar_subdir
        self.pub_models = pub_models
        self.routing_key_success = routing_key_success
        self.routing_key_fail = routing_key_fail

        _config.LOG_HANDLER_LEVEL = pynetdicom_log_level

        self.ae_title = ae_title
        self.hostname = hostname
        self.port = port

        self.assoc: Dict[AssocContext] = {}  # container for SCPContexts
        if blacklisted_hosts:
            self.blacklisted_hosts = blacklisted_hosts
        else:
            self.blacklisted_hosts = []
        self.whitelisted_hosts = whitelisted_hosts

    def __del__(self):
        if self.ae is not None:
            self.ae.shutdown()

    def handle_established(self, event: Event):
        sender = Destination(host=event.assoc.requestor.address,
                             port=event.assoc.requestor.port,
                             ae_title=event.assoc.requestor._ae_title)

        self.logger.debug(f"Validating sender {sender}")
        # If in whitelist, it will always be let through
        if self.whitelisted_hosts:
            if sender.host not in self.whitelisted_hosts:
                self.logger.error("SCU hostname is not whitelisted - shall not pass")
                raise Exception("Not on whitelist")
            else:
                self.logger.debug("SCU host validated - you shall pass!")
                return 0x0000

        # If whitelist is not defined, storescu will come through if not on blacklist
        if sender.host in self.blacklisted_hosts:
            self.logger.error("SCU hostname is blacklisted - shall not pass")
            raise Exception("On blacklisted")
        else:
            self.logger.debug("SCU host validated - you shall pass!")
            return 0x0000

    def maybe_init_store(self, event):
        # Association id unique to this transaction
        # Set up all the thing
        assoc_id = event.assoc.native_id

        # Check if already created
        if assoc_id in self.assoc.keys():
            return

        ac = AssocContext()
        self.logger.info(f"Receiving dicom files")

        # Unwrap sender info
        sender = Destination(host=event.assoc.requestor.address,
                             port=event.assoc.requestor.port,
                             ae_title=event.assoc.requestor._ae_title)

        ac.flow_context.sender = sender

        # Add to assocs dict
        self.assoc[assoc_id] = ac

    def handle_store(self, event):
        self.maybe_init_store(event)
        """Handle EVT_C_STORE events."""
        assoc_id = event.assoc.native_id
        assoc_context = self.assoc[assoc_id]

        self.logger.debug(f"HANDLE_STORE")

        # Get data set from event
        ds = event.dataset

        # Add the File Meta Information
        ds.file_meta = event.file_meta

        # Add file metas so they can be shipped on
        path_in_tar = os.path.join("/", ".".join([ds.Modality, ds.SeriesInstanceUID, ds.SOPInstanceUID, "dcm"]))
        self.logger.debug(f"Writing dicom to path {path_in_tar}")

        assoc_context.flow_context.add_meta_row(path_in_tar, ds)

        with tempfile.TemporaryFile() as f:
            f.write(b'\x00' * 128)  # Write the preamble
            f.write(b'DICM')  # Write prefix
            write_file_meta_info(f, event.file_meta)  # Encode and write the File Meta Information
            f.write(event.request.DataSet.getvalue())  # Write the encoded dataset
            assoc_context.add_file_to_tar(path_in_tar, f)  ## does not need to seek(0)

        # Return a 'Success' status
        return 0x0000

    def handle_release(self, event: Event):
        self.maybe_release_storescp(event)
        return 0x0000

    def maybe_release_storescp(self, event):
        assoc_id = event.assoc.native_id
        if assoc_id not in self.assoc.keys():
            return

        assoc_context = self.assoc[assoc_id]
        self.logger.debug(f"HANDLE_RELEASE: {assoc_id}")

        try:
            self.logger.info(f"STORESCP PUBLISH CONTEXT")

            assoc_context.flow_context.src_uid = self.publish_file_context(assoc_context=assoc_context)
            self.publish_main_context(assoc_context=assoc_context)

            self.logger.info(f"STORESCP PUBLISH CONTEXT", )
            self.logger.debug(f"HANDLE_RELEASE: {assoc_id}")
        except Exception as e:
            self.logger.error(str(e))
            raise e
        finally:
            del self.assoc[assoc_id]

    def handle_echo(self, event):
        self.logger.debug(f"Replying to ECHO")
        return 0x0000

    def publish_main_context(self, assoc_context):
        for pub_model in self.pub_models:
            pub_context = PublishContext(
                routing_key=self.routing_key_success,
                exchange=pub_model.exchange,
                body=assoc_context.flow_context.model_dump_json().encode())

            self.mq_pub.add_publish_message(pub_model, pub_context)
            #self.mq_pub.publish_message_callback(pub_model, pub_context)

    def publish_file_context(self, assoc_context):
        assoc_context.tar.close()
        assoc_context.file.seek(0)
        return self.fs.post(assoc_context.file)

    def stop(self, signalnum=None, stack_frame=None):
        self.ae.shutdown()

    def start(self, blocking=True):
        handler = [
            (evt.EVT_C_ECHO, self.handle_echo),
            (evt.EVT_ESTABLISHED, self.handle_established),
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
        except OSError as ose:
            self.logger.error(f'Cannot start Association Entity servers')
            raise ose
        except Exception as e:
            self.logger.error(str(e))
            raise e
