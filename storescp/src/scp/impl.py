import os
import tarfile
import tempfile

from io import BytesIO
from queue import Queue
from typing import Dict, List

from pynetdicom import AE, evt, StoragePresentationContexts, _config


from DicomFlowLib.data_structures.contexts import SCPContext, PublishContext, PubModel
from DicomFlowLib.data_structures.flow import Destination
from DicomFlowLib.fs import FileStorageClient
from DicomFlowLib.log import CollectiveLogger


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


class SCP:
    def __init__(self, publish_queue: Queue, file_storage: FileStorageClient, ae_title: str, hostname: str, port: int,
                 logger: CollectiveLogger, pub_models: List[PubModel], pynetdicom_log_level: str,
                 tar_subdir: List[str]):
        self.logger = logger
        self.fs = file_storage
        self.ae = None
        self.tar_subdir = tar_subdir
        self.pub_models = pub_models

        _config.LOG_HANDLER_LEVEL = pynetdicom_log_level

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

        ac = AssocContext()
        self.logger.debug(f"HANDLE_ESTABLISHED", uid=ac.flow_context.uid, finished=False)

        # Unwrap sender info
        ac.flow_context.sender = Destination(host=event.assoc.requestor.address,
                                             port=event.assoc.requestor.port,
                                             ae_title=event.assoc.requestor._ae_title)
        # Add to assocs dict
        self.assoc[assoc_id] = ac
        self.logger.debug(f"HANDLE_ESTABLISHED", uid=ac.flow_context.uid, finished=True)

        return 0x0000

    def handle_store(self, event):

        """Handle EVT_C_STORE events."""
        assoc_id = event.assoc.native_id
        assoc_context = self.assoc[assoc_id]
        uid = assoc_context.flow_context.uid

        self.logger.debug(f"HANDLE_STORE", uid=uid, finished=False)

        # Get data set from event
        ds = event.dataset

        # Add the File Meta Information
        ds.file_meta = event.file_meta

        # Add file metas so they can be shipped on
        assoc_context.flow_context.add_meta(ds.to_json_dict())

        prefix = [ds.get(key=tag, default=tag) for tag in self.tar_subdir]
        self.logger.debug(f"File subdir {prefix}")
        path_in_tar = os.path.join("/", *prefix, ds.SOPInstanceUID + ".dcm")

        self.logger.debug(f"Writing dicom to path {path_in_tar}")

        try:
            with tempfile.TemporaryFile() as tf:
                ds.save_as(tf, write_like_original=False)
                assoc_context.add_file_to_tar(path_in_tar, tf)  ## does not need to seek(0)
        except Exception as e:
            self.logger.error(str(e))
            raise e
        self.logger.debug(f"HANDLE_STORE", uid=uid, finished=True)

        # Return a 'Success' status
        return 0x0000

    def publish_main_context(self, assoc_context):
        for pub_model in self.pub_models:
            publish_context = PublishContext(
                routing_key=pub_model.routing_key_success,
                exchange=pub_model.exchange,
                body=assoc_context.flow_context.model_dump_json().encode()
            )
            self.publish_queue.put(publish_context, block=True)

    def publish_file_context(self, assoc_context):
        assoc_context.tar.close()
        assoc_context.file.seek(0)
        return self.fs.post(assoc_context.file)

    def handle_release(self, event):
        assoc_id = event.assoc.native_id
        assoc_context = self.assoc[assoc_id]
        uid = assoc_context.flow_context.uid
        self.logger.debug(f"HANDLE_RELEASE: {assoc_id}", finished=False)

        try:
            self.logger.info(f"STORESCP PUBLISH CONTEXT", uid=uid, finished=False)

            input_file_uid = self.publish_file_context(assoc_context=assoc_context)
            self.assoc[assoc_id].flow_context.input_file_uid = input_file_uid

            self.publish_main_context(assoc_context=assoc_context)

            self.logger.info(f"STORESCP PUBLISH CONTEXT", uid=uid, finished=True)
            self.logger.debug(f"HANDLE_RELEASE: {assoc_id}", finished=True)
        except Exception as e:
            self.logger.error(str(e))
            raise e
        finally:
            del self.assoc[assoc_id]

    def handle_echo(self, event):
        self.logger.info(f"Replying to ECHO", finished=True)
        return 0x0000

    def stop(self):
        self.ae.shutdown()

    def start(self, blocking=True):
        handler = [
            (evt.EVT_C_ECHO, self.handle_echo),
            (evt.EVT_ESTABLISHED, self.handle_established),
            (evt.EVT_C_STORE, self.handle_store),
            (evt.EVT_RELEASED, self.handle_release),
        ]

        try:
            self.logger.debug(
                f"Starting SCP on host: {self.hostname}, port:{str(self.port)}, ae title: {self.ae_title}",
                finished=False)

            # Create and run
            self.ae = AE(ae_title=self.ae_title)
            self.ae.supported_contexts = StoragePresentationContexts
            self.ae.maximum_pdu_size = 0
            self.ae.start_server((self.hostname, self.port), block=blocking, evt_handlers=handler)
            self.logger.debug(
                f"Starting SCP on host: {self.hostname}, port:{str(self.port)}, ae title: {self.ae_title}",
                finished=False)
        except OSError as ose:
            self.logger.error(f'Cannot start Association Entity servers')
            raise ose
        except Exception as e:
            self.logger.error(str(e))
            raise e
