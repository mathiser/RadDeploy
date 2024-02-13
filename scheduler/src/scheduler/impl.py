import json
import logging
from typing import Iterable, Tuple

import pika.spec

from DicomFlowLib.data_structures.contexts import FlowContext, PublishContext
from .db import Database
from .db.db_models import MountMapping


class Scheduler:
    def __init__(self,
                 pub_routing_key_success: str,
                 pub_routing_key_fail: str,
                 pub_routing_key_gpu: str,
                 pub_routing_key_cpu: str,
                 reschedule_priority: int,
                 consumer_exchange: str,
                 database_path: str,
                 log_level: int = 20):
        self.pub_declared = False
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)

        self.reschedule_priority = reschedule_priority
        self.consumer_exchange = consumer_exchange

        self.pub_routing_key_gpu = pub_routing_key_gpu
        self.pub_routing_key_cpu = pub_routing_key_cpu
        self.pub_routing_key_success = pub_routing_key_success
        self.pub_routing_key_fail = pub_routing_key_fail

        self.db = Database(database_path=database_path, log_level=log_level)

    def mq_entrypoint(self, basic_deliver, body) -> Iterable[PublishContext]:
        fc = FlowContext(**json.loads(body.decode()))
        fc = self.sync_mount_mapping(fc)

        priority = self.determine_priority(fc, basic_deliver)
        self.logger.debug(f"Setting priority from {fc.flow.priority} to {priority}")

        for new_fc, routing_key in self.schedule_from_flow_context(fc):
            yield PublishContext(body=new_fc.model_dump_json().encode(),
                                 routing_key=routing_key,
                                 priority=priority)

    def determine_priority(self, flow_context: FlowContext, basic_deliver: pika.spec.Basic.Deliver):
        if basic_deliver.exchange == self.consumer_exchange and basic_deliver.routing_key:
            return self.reschedule_priority
        else:
            return flow_context.flow.priority

    def schedule_from_flow_context(self, fc: FlowContext) -> Iterable[Tuple[FlowContext, str]]:
        if self.check_flow_finished(fc.uid):
            yield fc, self.pub_routing_key_success
            self.db.delete_all_by_uid(fc.uid)
        else:
            for new_fc in self.yield_eligible_models_as_publish_contexts(fc):
                if new_fc.active_model.gpu:
                    yield new_fc, self.pub_routing_key_gpu
                else:
                    yield new_fc, self.pub_routing_key_cpu

    def sync_mount_mapping(self, fc: FlowContext):
        for name, file_uid in fc.mount_mapping.items():
            self.db.insert_mount_mapping(uid=fc.uid,
                                         name=name,
                                         file_uid=file_uid)
        mappings = self.db.get_objs_by_kwargs(_obj_type=MountMapping, uid=fc.uid)
        fc.mount_mapping = {m.name: m.file_uid for m in mappings}
        return fc

    def check_flow_finished(self, flow_context_uid: str):
        return bool(self.db.get_objs_by_kwargs(_obj_type=MountMapping,
                                               uid=flow_context_uid,
                                               name="dst").first())

    def evaluate_mount_keys_are_in_db(self, uid, mount_keys):
        for mount_key in mount_keys:
            if not self.db.get_objs_by_kwargs(MountMapping, uid=uid, name=mount_key).first():
                return False
        else:
            return True

    def evaluate_mount_keys_are_not_in_db(self, uid, mount_keys):
        for mount_key in mount_keys:
            if self.db.get_objs_by_kwargs(MountMapping, uid=uid, name=mount_key).first():
                return False
        else:
            return True

    def yield_eligible_models_as_publish_contexts(self, fc: FlowContext) -> Iterable[FlowContext]:
        uid = fc.uid
        for i, model in enumerate(fc.flow.models):
            try:
                self.db.insert_dispatched_model(uid=uid, model_id=i)  # Checks if this task has already been dispatched
                if self.evaluate_mount_keys_are_in_db(uid=uid,
                                                      mount_keys=model.input_mount_keys):  # Check that inputs exist
                    if self.evaluate_mount_keys_are_not_in_db(uid=uid,
                                                              mount_keys=model.output_mount_keys):  # Check that outputs not exist
                        new_fc: FlowContext = fc.model_copy(deep=True)
                        new_fc.active_model_idx = i
                        yield new_fc
            except Exception as e:
                print(e)
                self.logger.debug(f"Model {i} for flow {uid} is already dispatched")
