import json
from typing import Iterable, Dict, List

from DicomFlowLib.data_structures.contexts import FlowContext
from DicomFlowLib.data_structures.mq import MQEntrypointResult
from DicomFlowLib.log import CollectiveLogger
from .db import Database


class FlowTracker:
    def __init__(self,
                 logger: CollectiveLogger,
                 database_path: str,
                 dashboard_rules: List[Dict]):
        self.logger = logger
        self.engine = None
        self.database_url = None
        self.dashboard_rules = dashboard_rules
        self.database_path = database_path
        self.db = Database(database_path=self.database_path)

    def mq_entrypoint(self, basic_deliver, body) -> Iterable[MQEntrypointResult]:
        context = FlowContext(**json.loads(body.decode()))
        self.update_dashboard_rows(basic_deliver, context)
        return []

    def update_dashboard_rows(self, basic_deliver, context):
        self.db.maybe_insert_row(uid=context.flow_instance_uid,
                                 name=context.flow.name,
                                 sender=context.sender.host,
                                 priority=context.flow.priority)

        for rule in self.dashboard_rules:
            if basic_deliver.exchange in [rule["on_exchange"], "#"]:
                if basic_deliver.routing_key in [rule["on_routing_key"], "#"]:
                    self.db.set_status_of_row(context.flow_instance_uid, rule["status"])