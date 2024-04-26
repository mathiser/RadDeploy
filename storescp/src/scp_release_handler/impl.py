import queue
from typing import List, Tuple

from DicomFlowLib.mq.mq_models import PublishContext
from DicomFlowLib.fs.client.interface import FileStorageClientInterface
from DicomFlowLib.mq import PubModel
from .interface import SCPReleaseHandlerInterface
from scp.models import SCPAssociation


class SCPReleaseHandler(SCPReleaseHandlerInterface):
    def __init__(self,
                 pub_models: List[PubModel],
                 file_storage: FileStorageClientInterface,
                 in_queue: queue.Queue[SCPAssociation],
                 out_queue: queue.Queue[Tuple[PubModel, PublishContext]]):

        super().__init__()

        self.in_queue = in_queue
        self.out_queue = out_queue
        self.pub_models = pub_models
        self.fs = file_storage
        self.running = False

    def run(self):
        self.running = True
        while self.running:
            try:
                scp_association = self.in_queue.get(timeout=10)
                self.release(scp_association)
            except queue.Empty:
                pass

    def release(self, scp_association: SCPAssociation):
        scp_association.src_uid = self.fs.post(scp_association.as_tar())
        for pub_model in self.pub_models:
            pub_context = PublishContext(
                pub_model_routing_key="SUCCESS",
                exchange=pub_model.exchange,
                body=scp_association.model_dump_json(exclude={"dicom_files"}).encode())

            self.out_queue.put((pub_model, pub_context))  # These are for the MQPub

    def stop(self):
        self.running = False
