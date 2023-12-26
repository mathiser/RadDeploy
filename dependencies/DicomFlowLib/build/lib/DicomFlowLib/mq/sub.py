# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205
import logging
import threading
import traceback

from python_logging_rabbitmq import RabbitMQHandler

from DicomFlowLib.default_config import LOG_FORMAT
from .base import MQBase


class MQSub(MQBase):
    """This is an example consumer that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, this class will stop and indicate
    that reconnection is necessary. You should look at the output, as
    there are limited reasons why the connection may be closed, which
    usually are tied to permission related issues or socket timeouts.

    If the channel is closed, it will indicate a problem with one of the
    commands that were issued and that should surface in the output as well.

    """

    def __init__(self, hostname: str, port: int, log_level: int,
                 routing_key: str, routing_key_as_queue: bool,
                 work_function: callable, prefetch_value: int,
                 mq_logger: RabbitMQHandler | None = None,
                 exchange: str = "", exchange_type: str = "direct",
                 ):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with

        """
        super().__init__(mq_logger, hostname, port, log_level)
        logging.basicConfig(level=log_level, format=LOG_FORMAT)
        self.LOGGER = logging.getLogger(__name__)
        self.mq_logger = mq_logger
        if mq_logger:
            self.LOGGER.addHandler(mq_logger)

        self.should_reconnect = False
        self.was_consuming = False

        self._consumer_tag = None
        self._consuming = False

        self.exchange = exchange
        self.exchange_type = exchange_type
        self.routing_key = routing_key
        self.routing_key_as_queue = routing_key_as_queue
        self.work_function = work_function
        self.prefetch_value = prefetch_value

        self._thread_lock = threading.Lock()
        self._threads = []

    def run(self):
        # Set Connection and channel
        self.connect()

        # Declare exchange
        self.setup_exchange(exchange=self.exchange, exchange_type=self.exchange_type)

        # Declare queue
        queue = self.setup_queue_and_bind(exchange=self.exchange,
                                          routing_key=self.routing_key,
                                          routing_key_as_queue=self.routing_key_as_queue)

        # Set prefetch value
        self._channel.basic_qos(prefetch_count=self.prefetch_value)

        self._channel.basic_consume(queue=queue, on_message_callback=self.on_message)

        self.LOGGER.info(' [*] Waiting for messages')
        self._channel.start_consuming()

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        self.LOGGER.info('Received message # %s from %s', basic_deliver.delivery_tag, properties.app_id)

        t = threading.Thread(target=self.work_function_wrapper,
                             args=(self._connection, self._channel, basic_deliver, properties, body))
        t.start()
        self._threads.append(t)

    def work_function_wrapper(self, connection, channel, basic_deliver, properties, body):
        try:
            self.LOGGER.info('Starting work function of message # %s', basic_deliver.delivery_tag)
            self.work_function(connection, channel, basic_deliver, properties, body)
            self.LOGGER.info('Finishing work function of message # %s', basic_deliver.delivery_tag)
        except Exception as e:
            # self.LOGGER.error(str(e))
            self.LOGGER.error(str(traceback.format_exc()))
            # raise e
        finally:
            self.LOGGER.info('Acknowledging message %s', basic_deliver.delivery_tag)
            self.acknowledge_message_callback(basic_deliver.delivery_tag)
