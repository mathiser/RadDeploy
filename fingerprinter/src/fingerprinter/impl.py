import json
import logging
import tarfile
from typing import Iterable

from DicomFlowLib.data_structures.contexts import FlowContext, PublishContext, SCPContext
from DicomFlowLib.fs import FileStorageClient
from .fp_utils import parse_fingerprints, slice_dataframe_to_triggers, generate_flow_specific_tar


class Fingerprinter:
    def __init__(self,
                 file_storage: FileStorageClient,
                 flow_directory: str,
                 routing_key_success: str,
                 routing_key_fail: str,
                 log_level: int = 20):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)

        self.flow_directory = flow_directory
        self.fs = file_storage
        self.uid = None
        self.routing_key_success = routing_key_success
        self.routing_key_fail = routing_key_fail

    def mq_entrypoint(self, basic_deliver, body) -> Iterable[PublishContext]:
        results = []
        self.logger.info(self.logger.name)

        scp_context = SCPContext(**json.loads(body.decode()))
        self.uid = scp_context.uid
        tar_file = self.fs.get(scp_context.src_uid)
        try:
            for flow in parse_fingerprints(self.flow_directory):
                sliced_dataframed = slice_dataframe_to_triggers(scp_context.dataframe, flow.triggers)
                if bool(len(sliced_dataframed)):
                    self.logger.info(f"MATCHING FLOW")

                    with generate_flow_specific_tar(sliced_df=sliced_dataframed, tar_file=tar_file) as flow_tar:
                        src_uid = self.fs.post(flow_tar)

                    flow_context = FlowContext(flow=flow.copy(deep=True),
                                               src_uid=src_uid,
                                               dataframe=sliced_dataframed.copy(deep=True),
                                               sender=scp_context.sender)
                    self.logger.info(str(flow_context))
                    # Publish the context
                    results.append(
                        PublishContext(routing_key=self.routing_key_success,
                                       body=flow_context.model_dump_json().encode(),
                                       priority=flow.priority)
                    )

                else:
                    self.logger.info(f"NOT MATCHING FLOW")
                    results.append(
                        PublishContext(routing_key=self.routing_key_fail,
                                       body=scp_context.model_dump_json().encode(),
                                       priority=flow.priority))

            return results
        except Exception as e:
            raise e
        finally:
            tar_file.close()
            self.uid = None
