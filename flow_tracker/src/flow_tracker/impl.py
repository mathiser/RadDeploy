import json

from DicomFlowLib.data_structures.contexts import FlowContext
from DicomFlowLib.log import CollectiveLogger
from DicomFlowLib.mq import MQBase


class FlowTracker(MQBase):
    def __init__(self,
                 rabbit_hostname: str,
                 rabbit_port: int,
                 logger: CollectiveLogger,
                 sub_exchanges: str,
                 sub_prefetch_value: int):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with

        """
        super().__init__(logger=logger,
                         close_conn_on_exit=True,
                         rabbit_hostname=rabbit_hostname,
                         rabbit_port=rabbit_port)

        self.logger = logger

        self.should_reconnect = False
        self.was_consuming = False

        self._consumer_tag = None
        self._consuming = False

        self.sub_exchanges = sub_exchanges.split()

        self.sub_prefetch_value = sub_prefetch_value
        self.queue = None
    def run(self):
        # Set Connection and channel
        self.connect()

        self.queue = self.setup_queue(queue="")

        # Declare exchange
        for exchange in self.sub_exchanges:
            self.setup_exchange(exchange=exchange, exchange_type="fanout")
            self.bind_queue(self.queue, exchange=exchange, routing_key="")

        # Set prefetch value
        self._channel.basic_qos(prefetch_count=self.sub_prefetch_value)

        self._channel.basic_consume(queue=self.queue, on_message_callback=self.on_message)

        self.logger.info(' [*] Waiting for messages')
        self._channel.start_consuming()


    def on_message(self, _unused_channel, basic_deliver, properties, body):
        try:
            self.logger.debug('Starting work function of message # {}'.format(basic_deliver.delivery_tag))
            self.work_function(self._connection, self._channel, basic_deliver, properties, body)
            self.logger.debug('Finishing work function of message # {}'.format(basic_deliver.delivery_tag))
        except Exception as e:
            self.logger.error(str(e))
        finally:
            self.logger.info('Acknowledging message {}'.format(basic_deliver.delivery_tag))
            self.acknowledge_message_callback(basic_deliver.delivery_tag)

    def work_function(self, connection, channel, basic_deliver, properties, body):
        context = FlowContext(**json.loads(body.decode()))
        context.file_metas = []
        print(basic_deliver)
        print(context)

