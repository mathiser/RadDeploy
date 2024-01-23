import json

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

        event = self.db.add_event(exchange=basic_deliver.exchange,
                                  routing_key=basic_deliver.routing_key,
                                  context=context)
        self.file_janitor(event)
        self.dashboard_janitor(event, context)

    def file_janitor(self, event):
        if event.exchange == "storescu":
            self.db.delete_files_by_id(id=event.id)
        elif event.exchange == "fingerprinter":
            self.db.delete_all_files_by_kwargs(uid=event.uid, exchange="storescp")
        elif event.routing_key == "fail":
            self.db.delete_all_files_by_kwargs(id=event.id)

    def dashboard_janitor(self, event, context):
        self.db.maybe_insert_dashboard_row(flow_instance_uid=event.flow_instance_uid,
                                           flow_container_tag=context.flow.model.docker_kwargs["image"],
                                           sender_ae_hostname=context.sender.host)
        if event.routing_key == "fail":
            self.db.set_status_of_dashboard_row(flow_instance_uid=event.flow_instance_uid, status=400)
        elif event.exchange == "fingerprinter":
            self.db.set_status_of_dashboard_row(flow_instance_uid=event.flow_instance_uid, status=0)
        # elif event.exchange == "consumer":
        #     self.db.set_status_of_dashboard_row(flow_instance_uid=event.flow_instance_uid, 2)  # space for dispatched job
        elif event.exchange == "consumer":
            self.db.set_status_of_dashboard_row(flow_instance_uid=event.flow_instance_uid, status=2)
        elif event.exchange == "storescu":
            self.db.set_status_of_dashboard_row(flow_instance_uid=event.flow_instance_uid, status=3)
        else:
            pass
