import queue
import threading
from typing import List, Tuple

from DicomFlowLib.data_structures.contexts import PublishContext
from DicomFlowLib.fs.client.interface import FileStorageClientInterface
from DicomFlowLib.mq import PubModel, MQPub
from .interface import SCPReleaseHandlerInterface
from scp.models import SCPAssociation


class SCPReleaseHandler(SCPReleaseHandlerInterface):
    def __init__(self,
                 pub_models: List[PubModel],
                 file_storage: FileStorageClientInterface,
                 in_queue: queue.Queue[SCPAssociation],
                 out_queue: queue.Queue[Tuple[PubModel, PublishContext]],
                 mq: MQPub):

        super().__init__()

        self.in_queue = in_queue
        self.out_queue = out_queue
        self.pub_models = pub_models
        self.fs = file_storage
        self.mq = mq
        self.running = False

    def run(self):
        while self.running:
            try:
                scp_association = self.in_queue.get(timeout=10)
                self.release(scp_association)
            except queue.Empty:
                pass

    def release(self, scp_association: SCPAssociation):
        scp_association.file_uuid = self.fs.post(scp_association.as_tar())
        for pub_model in self.pub_models:
            pub_context = PublishContext(
                routing_key=pub_model.routing_key_success,
                exchange=pub_model.exchange,
                body=scp_association.model_dump_json(exclude={"dicom_files"}).encode())

            self.out_queue.put((pub_model, pub_context))  # These are for the MQPub
            yield pub_context


