import json
from typing import Iterable, Dict, List

from DicomFlowLib.data_structures.contexts import FlowContext, PublishContext
from DicomFlowLib.fs import FileStorageClient
from DicomFlowLib.log import CollectiveLogger
from .db import Database


class Janitor:
    def __init__(self,
                 file_storage: FileStorageClient,
                 logger: CollectiveLogger,
                 database_path: str,
                 file_delete_rules: List[Dict]):
        self.logger = logger
        self.engine = None
        self.database_url = None
        self.file_delete_rules = file_delete_rules
        self.database_path = database_path
        self.db = Database(logger=self.logger, database_path=self.database_path, file_storage=file_storage)

    def mq_entrypoint(self, basic_deliver, body) -> Iterable[PublishContext]:
        self.logger.info(f"Received from {basic_deliver.exchange} on {basic_deliver.routing_key}")
        context = FlowContext(**json.loads(body.decode()))
        context.file_metas = []

        event = self.db.add_event(exchange=basic_deliver.exchange,
                                  routing_key=basic_deliver.routing_key,
                                  context=context)
        self.file_janitor(event)
        return []

    def file_janitor(self, event):
        for rule in self.file_delete_rules:
            if (event.exchange == rule["on_exchange"]) or (rule["on_exchange"] == "#"):
                if (event.routing_key == rule["on_routing_key"]) or (rule["on_routing_key"] == "#"):
                    kwargs = {}
                    for k, v in rule["event_kwargs"].items():
                        try:
                            kwargs[k] = event.__dict__[v]
                        except Exception as e:
                            print(e)
                            kwargs[k] = v
                    print(kwargs)
                    if rule["delete_input_file"]:
                        self.db.delete_input_files_by_kwargs(**kwargs)
                    if rule["delete_output_file"]:
                        self.db.delete_output_files_by_kwargs(**kwargs)