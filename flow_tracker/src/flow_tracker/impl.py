import json
from typing import List

from DicomFlowLib.data_structures.contexts import FlowContext, PubModel
from DicomFlowLib.log import CollectiveLogger
from DicomFlowLib.mq import MQSub, MQSubEntrypoint


class FlowTracker(MQSubEntrypoint):
    def __init__(self,
                 logger: CollectiveLogger,
                 pub_models: List[PubModel]):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with

        """

        super().__init__(logger, pub_models)
        self.logger = logger

    def mq_entrypoint(self, connection, channel, basic_deliver, properties, body):
        context = FlowContext(**json.loads(body.decode()))
        context.file_metas = []
        print(basic_deliver)
        print(context)

