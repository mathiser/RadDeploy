import json
import logging
from typing import Iterable

from DicomFlowLib.data_structures.contexts import FlowContext, PublishContext, SCPContext
from DicomFlowLib.fs import FileStorageClient
from .fp_utils import parse_fingerprints, slice_dataframe_to_triggers, generate_flow_specific_tar, generate_df_from_tar


class Fingerprinter:
    def __init__(self,
                 file_storage: FileStorageClient,
                 flow_directory: str,
                 log_level: int = 20):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)

        self.flow_directory = flow_directory
        self.fs = file_storage
        self.uid = None


    def mq_entrypoint(self, basic_deliver, body) -> Iterable[PublishContext]:
        results = []
        self.logger.info(self.logger.name)

        scp_context = SCPContext(**json.loads(body.decode()))
        self.uid = scp_context.uid
        tar_file = self.fs.get(scp_context.src_uid)
        dataframe = generate_df_from_tar(tar_file=tar_file)
        try:
            for flow in parse_fingerprints(self.flow_directory):
                sliced_dataframe = slice_dataframe_to_triggers(dataframe, flow.triggers)

                if sliced_dataframe is not None:  # If there is a match
                    self.logger.info(f"MATCHING FLOW")

                    flow_tar_file = generate_flow_specific_tar(sliced_df=sliced_dataframe,
                                                               tar_file=tar_file,
                                                               tar_subdir=flow.tar_subdir)

                    flow_context = FlowContext(flow=flow.copy(deep=True),
                                               src_uid=self.fs.post(flow_tar_file),
                                               dataframe=sliced_dataframe.copy(deep=True),
                                               sender=scp_context.sender)
                    flow_tar_file.close()

                    self.logger.info(str(flow_context))

                    # Publish the context
                    results.append(
                        PublishContext(pub_model_routing_key="SUCCESS",
                                       body=flow_context.model_dump_json().encode(),
                                       priority=flow.priority)
                    )

                else:
                    self.logger.info(f"NOT MATCHING FLOW")
                    results.append(
                        PublishContext(routing_key="FAIL",
                                       body=scp_context.model_dump_json().encode(),
                                       priority=flow.priority))

            return results
        except Exception as e:
            raise e
        finally:
            tar_file.close()
            self.uid = None
