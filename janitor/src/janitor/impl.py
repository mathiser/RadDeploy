import json
import os

from DicomFlowLib.data_structures.contexts import FlowContext
from DicomFlowLib.fs import FileStorage
from DicomFlowLib.log import CollectiveLogger
from DicomFlowLib.mq import MQSubEntrypoint
from .db import Database


class Janitor(MQSubEntrypoint):
    def __init__(self,
                 file_storage: FileStorage,
                 logger: CollectiveLogger,
                 database_path: str):
        super().__init__(logger=logger, pub_models=None)
        self.engine = None
        self.database_url = None

        self.database_path = database_path
        self.db = Database(database_path=self.database_path, file_storage=file_storage)

    def mq_entrypoint(self, connection, channel, basic_deliver, properties, body):
        context = FlowContext(**json.loads(body.decode()))
        context.file_metas = []
        event = self.db.add_event(uid=context.uid,
                                  exchange=basic_deliver.exchange,
                                  routing_key=basic_deliver.routing_key,
                                  context=context)

        self.file_janitor(event)

    def file_janitor(self, event):
        if event.exchange == "storescu":
            if (self.db.get_events_by_kwargs(uid=event.uid, exchange="storescu").count()
                    == self.db.get_events_by_kwargs(uid=event.uid, exchange="fingerprinter").count()
                    != 0):
                self.db.delete_all_files_by_uid(event.uid)
