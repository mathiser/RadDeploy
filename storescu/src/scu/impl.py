import json
import logging
import os
import tarfile
import tempfile
import time
from typing import Iterable, List

import pydicom
from pydicom.errors import InvalidDicomError
from pynetdicom import AE, StoragePresentationContexts

from RadDeployLib.data_structures.flow import Destination
from RadDeployLib.data_structures.service_contexts import FlowContext, FinishedFlowContext
from RadDeployLib.fs import FileStorageClient
from RadDeployLib.mq.mq_models import PublishContext, PubModel

from RadDeployLib.data_structures.service_contexts.models import SCUContext


class SCU:
    def __init__(self,
                 file_storage: FileStorageClient,
                 ae_title: str,
                 ae_port: int,
                 ae_hostname: str,
                 log_level: int = 20):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        self.fs = file_storage
        self.ae_title = ae_title
        self.ae_port = ae_port
        self.ae_hostname = ae_hostname

    def mq_entrypoint(self, basic_deliver, body) -> Iterable[PublishContext]:
        finished_flow_context = FinishedFlowContext(**json.loads(body.decode()))
        self.logger.info(finished_flow_context.model_dump_json())

        sender_destinations = []
        for port in finished_flow_context.flow.return_to_sender_on_ports:
            sender_destinations.append(Destination(host=finished_flow_context.sender.host,
                                                   port=port,
                                                   ae_title=finished_flow_context.sender.ae))

        all_destinations = finished_flow_context.flow.destinations + sender_destinations
        for src, uid in finished_flow_context.mount_mapping.items():
            if src.startswith("dst"):

                for dest, status in self.post_file_on_destinations(uid, all_destinations):
                    scu_context = SCUContext(finished_flow_context=finished_flow_context,
                                             destination=dest,
                                             status=status)
                    pub_model_routing_key = "SUCCESS" if status else "FAIL"

                    yield PublishContext(body=scu_context.model_dump_json().encode(),
                                         pub_model_routing_key=pub_model_routing_key)
                    self.logger.debug(f"Completed sending {pub_model_routing_key} {scu_context}")

    def post_file_on_destinations(self, file_uid, destinations):
        self.logger.debug("EXTRACTING FILE(S)")
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_tar = self.fs.get(file_uid)

            with tarfile.TarFile.open(fileobj=output_tar, mode="r:*") as tf:
                tf.extractall(tmp_dir, filter="data")

            for dest in destinations:
                self.logger.info(f"POSTING TO {dest.ae_title} ON: {dest.host}:{dest.port}")
                yield dest, self.post_folder_to_dicom_node(dicom_dir=tmp_dir, destination=dest)

    def post_folder_to_dicom_node(self, dicom_dir, destination: Destination) -> bool:
        ae = AE(ae_title=self.ae_title)
        ae.requested_contexts = StoragePresentationContexts

        assoc = ae.associate(destination.host, destination.port, ae_title=destination.ae_title,
                             bind_address=(self.ae_hostname, self.ae_port))
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
                        pass
                    except Exception as e:
                        self.logger.error(f"{p}, {str(e)}")

            # Release the association
            self.logger.debug('Releasing Association')
            assoc.release()
            return True
        else:
            self.logger.error('Association rejected, aborted or never connected')
            return False
