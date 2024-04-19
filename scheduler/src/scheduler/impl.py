import json
import logging
from typing import Iterable, Dict, Tuple, List

import pika.spec

from DicomFlowLib.mq.mq_models import PublishContext


class Scheduler:
    def __init__(self,
                 pub_routing_key_success: str,
                 pub_routing_key_fail: str,
                 pub_routing_key_gpu: str,
                 pub_routing_key_cpu: str,
                 reschedule_priority: int,
                 consumer_exchange: str,
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
        self.mount_mapping: Dict[str, Dict[str, str]] = {}
        self.dispatched_flows: Dict[str, List[int]] = {}

    def mq_entrypoint(self, basic_deliver, body) -> Iterable[PublishContext]:
        fc = FlowContext(**json.loads(body.decode()))

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
        fc = self.update_mount_mapping(fc)
        if self.check_flow_finished(fc):
            yield fc, self.pub_routing_key_success
            del self.mount_mapping[fc.uid]
        else:
            for new_fc in self.yield_eligible_models_as_publish_contexts(fc):
                if new_fc.active_model.gpu:
                    yield new_fc, self.pub_routing_key_gpu
                else:
                    yield new_fc, self.pub_routing_key_cpu

    def update_mount_mapping(self, fc: FlowContext):
        if fc.uid not in self.mount_mapping.keys():
            self.mount_mapping[fc.uid] = {}

        self.mount_mapping[fc.uid] = {**self.mount_mapping[fc.uid], **fc.mount_mapping}
        fc.mount_mapping = self.mount_mapping[fc.uid]
        return fc

    def get_mapping_by_uid(self, flow_context_uid: str):
        return self.mount_mapping[flow_context_uid]

    def get_mapping_keys_by_uid(self, flow_context_uid: str):
        return set(self.get_mapping_by_uid(flow_context_uid).keys())

    @staticmethod
    def check_flow_finished(fc: FlowContext):
        return "dst" in fc.mount_mapping.keys()

    def yield_eligible_models_as_publish_contexts(self, fc: FlowContext) -> Iterable[FlowContext]:
        uid = fc.uid
        for i, model in enumerate(fc.flow.models):

            # Checks if this task has already been dispatched
            if self.is_already_scheduled(i, uid):
                continue

            if model.input_mount_keys.issubset(self.get_mapping_keys_by_uid(uid)):
                if not model.output_mount_keys.issubset(self.get_mapping_keys_by_uid(uid)):
                    new_fc: FlowContext = fc.model_copy(deep=True)
                    new_fc.active_model_idx = i
                    self.dispatched_flows[uid].append(i)
                    yield new_fc

    def is_already_scheduled(self, i, uid):
        if uid not in self.dispatched_flows.keys():
            self.dispatched_flows[uid] = []
            return False

        return i in self.dispatched_flows[uid]
