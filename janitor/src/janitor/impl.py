import json
from typing import Iterable

from DicomFlowLib.data_structures.contexts import FlowContext
from DicomFlowLib.data_structures.mq import MQEntrypointResult
from DicomFlowLib.fs import FileStorageClient
from DicomFlowLib.log import CollectiveLogger
from .db import Database


class Janitor:
    def __init__(self,
                 file_storage: FileStorageClient,
                 logger: CollectiveLogger,
                 database_path: str):
        self.logger = logger
        self.engine = None
        self.database_url = None

        self.database_path = database_path
        self.db = Database(logger=self.logger, database_path=self.database_path, file_storage=file_storage)

    def mq_entrypoint(self, basic_deliver, body) -> Iterable[MQEntrypointResult]:
        context = FlowContext(**json.loads(body.decode()))
        context.file_metas = []

        event = self.db.add_event(exchange=basic_deliver.exchange,
                                  routing_key=basic_deliver.routing_key,
                                  context=context)
        self.file_janitor(event)
        return []

    def file_janitor(self, event):
        if event.exchange == "storescu":
            self.db.delete_files_by_id(id=event.id)
        elif event.exchange == "fp" and event.routing_key == "success":
            self.db.delete_all_files_by_kwargs(uid=event.uid, exchange="storescp")
        elif event.routing_key == "fail":
            self.db.delete_all_files_by_kwargs(id=event.id)

