import json
import logging
from io import BytesIO
from typing import Iterable

from DicomFlowLib.data_structures.service_contexts import FlowContext, SCPContext
from DicomFlowLib.fs.client.interface import FileStorageClientInterface
from DicomFlowLib.mq import PublishContext
from .fp_utils import parse_fingerprints, slice_dataframe_to_triggers, generate_flow_specific_tar, generate_df_from_tar


class Fingerprinter:
    def __init__(self,
                 file_storage: FileStorageClientInterface,
                 flow_directory: str,
                 log_level: int = 20):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)

        self.flow_directory = flow_directory
        self.fs = file_storage

    def mq_entrypoint(self, basic_deliver, body) -> Iterable[PublishContext]:
        self.logger.info(self.logger.name)

        scp_context = SCPContext(**json.loads(body.decode()))
        tar_file = self.fs.get(scp_context.src_uid)
        try:
            return self.run_fingerprinting(scp_context=scp_context, tar_file=tar_file)
        except Exception as e:
            self.logger.error(str(e))
            raise e
        finally:
            tar_file.close()

    def run_fingerprinting(self, scp_context: SCPContext, tar_file: BytesIO):
        results = []

        self.logger.info("Generating a dataframe of all dicom files in the tar")

        dataframe = generate_df_from_tar(tar_file=tar_file)
        self.logger.info(str(dataframe))
        for flow in parse_fingerprints(self.flow_directory):
            # Generate a dataframe of files which match the flow triggers
            self.logger.info("Slicing the dataframe to only dicom files which match the flow")
            sliced_dataframe = slice_dataframe_to_triggers(dataframe, flow.triggers)
            self.logger.info(str(sliced_dataframe))

            if sliced_dataframe is not None:  # If there is a match
                self.logger.info(f"MATCHING FLOW")
                flow_tar_file = generate_flow_specific_tar(sliced_df=sliced_dataframe,
                                                           tar_file=tar_file,
                                                           tar_subdir=flow.tar_subdir)
                flow_context = FlowContext(flow=flow.model_copy(deep=True),
                                           src_uid=self.fs.post(flow_tar_file),
                                           dataframe=sliced_dataframe.copy(deep=True),
                                           sender=scp_context.sender)
                flow_tar_file.close()

                self.logger.info(str(flow_context))

                # Publish the context
                results.append(PublishContext(pub_model_routing_key="SUCCESS",
                                     body=flow_context.model_dump_json().encode(),
                                     priority=flow.priority))

            else:
                self.logger.info(f"NOT MATCHING FLOW")
                results.append(PublishContext(pub_model_routing_key="FAIL",
                                     body=scp_context.model_dump_json().encode(),
                                     priority=flow.priority))

        return results