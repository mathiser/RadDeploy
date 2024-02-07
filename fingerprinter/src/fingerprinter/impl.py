import json
import logging
from typing import Iterable

from DicomFlowLib.data_structures.contexts import FlowContext, PublishContext, SCPContext
from DicomFlowLib.fs import FileStorageClient
from .fp_utils import parse_fingerprints, fingerprint, parse_file_metas


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

        ds = parse_file_metas(scp_context.file_metas)

        for flow in parse_fingerprints(self.flow_directory):
            if fingerprint(ds, flow.triggers):
                self.logger.info(f"MATCHING FLOW")
                flow_context = FlowContext(flow=flow.copy(deep=True),
                                           src_uid=self.fs.clone(scp_context.input_file_uid),
                                           dataframe_json=ds.to_json(),
                                           sender=scp_context.sender)

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
        self.uid = None
        return results
