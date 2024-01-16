from typing import List

from DicomFlowLib.data_structures.contexts import FlowContext, PubModel
from DicomFlowLib.log import CollectiveLogger
from DicomFlowLib.mq import MQBase


class MQSubEntrypoint:
    def __init__(self, logger: CollectiveLogger, pub_models: List[PubModel] | None = None):
        self.pub_declared = False
        self.logger = logger

        if not pub_models:
            self.pub_models = []
        else:
            self.pub_models = pub_models

    def _maybe_declare_exchange_and_queue(self, mq):
        if not self.pub_declared:
            for pub_model in self.pub_models:
                mq.setup_exchange_callback(exchange=pub_model.exchange,
                                           exchange_type=pub_model.exchange_type)
            self.pub_declared = True

    def publish(self, mq: MQBase, context: FlowContext):
        self._maybe_declare_exchange_and_queue(mq)
        for pub_model in self.pub_models:
            self.logger.info(f"PUB TO QUEUE: {pub_model.routing_key_success}", uid=context.uid, finished=False)
            mq.basic_publish_callback(exchange=pub_model.exchange,
                                      routing_key=pub_model.routing_key_success,
                                      body=context.model_dump_json().encode(),
                                      priority=context.flow.priority)
            self.logger.info(f"PUB TO QUEUE: {pub_model.routing_key_success}", uid=context.uid, finished=True)
