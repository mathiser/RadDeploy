import json
from typing import Iterable

from DicomFlowLib.data_structures.contexts import FlowContext, PublishContext
from DicomFlowLib.fs import FileStorageClient
from DicomFlowLib.log import CollectiveLogger
from DicomFlowLib.fp import parse_fingerprints, fingerprint, parse_file_metas


class Fingerprinter:
    def __init__(self,
                 file_storage: FileStorageClient,
                 logger: CollectiveLogger,
                 flow_directory: str,
                 routing_key_success: str,
                 routing_key_fail: str
                 ):
        self.logger = logger
        self.flow_directory = flow_directory
        self.fs = file_storage
        self.uid = None
        self.routing_key_success = routing_key_success
        self.routing_key_fail = routing_key_fail

    def mq_entrypoint(self, basic_deliver, body) -> Iterable[PublishContext]:
        results = []
        org_context = FlowContext(**json.loads(body.decode()))
        self.uid = org_context.uid

        ds = parse_file_metas(org_context.file_metas)

        for flow in parse_fingerprints(self.flow_directory):
            if fingerprint(ds, flow.triggers):
                self.logger.info(f"MATCHING FLOW", uid=self.uid, finished=True)
                context = org_context.model_copy(deep=True)
                context.add_flow(flow.model_copy(deep=True))

                # Provide a link to the input file
                context.input_file_uid = self.fs.clone(context.input_file_uid)

                # Publish the context
                results.append(
                    PublishContext(routing_key=self.routing_key_success,
                                   body=context.model_dump_json().encode(),
                                   priority=flow.priority)
                )

            else:
                self.logger.info(f"NOT MATCHING FLOW", uid=self.uid, finished=True)
                results.append(
                    PublishContext(routing_key=self.routing_key_fail,
                                   body=org_context.model_dump_json().encode(),
                                   priority=flow.priority)
                )
        self.uid = None
        return results
