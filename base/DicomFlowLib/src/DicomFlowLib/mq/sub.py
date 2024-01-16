# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205
import threading
import traceback
from typing import List, Dict

from DicomFlowLib.log import CollectiveLogger
from .base import MQBase
from ..data_structures.contexts.pub_context import SubModel



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

    def __init__(self,
                 rabbit_hostname: str,
                 rabbit_port: int,
                 sub_models: List[SubModel],
                 work_function: callable,
                 sub_prefetch_value: int,
                 logger: CollectiveLogger,
                 sub_queue_kwargs: Dict,
                 ):
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

        self.work_function = work_function
        self.sub_prefetch_value = sub_prefetch_value
        self._thread_lock = threading.Lock()
        self._threads = []

        self.sub_bases = sub_models
        self.sub_queue_kwargs = sub_queue_kwargs
        self.queue = None

    def run(self):
        # Set Connection and channel
        self.connect()
        self.queue = self.setup_queue(**self.sub_queue_kwargs)

        # Declare exchange
        for sub in self.sub_bases:
            self.setup_exchange(exchange=sub.exchange, exchange_type=sub.exchange_type)

            # Declare queue
            self.bind_queue(queue=self.queue, exchange=sub.exchange, routing_key=sub.routing_key)

        # Set prefetch value
        self._channel.basic_qos(prefetch_count=self.sub_prefetch_value)

        # Set consuming queue
        self._channel.basic_consume(queue=self.queue, on_message_callback=self.on_message)

        self.logger.info(' [*] Waiting for messages')
        self._channel.start_consuming()

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        self.logger.debug('Received message # {} from {}'.format(basic_deliver.delivery_tag, properties.app_id))

        t = threading.Thread(target=self.work_function_wrapper,
                             args=(self._connection, self._channel, basic_deliver, properties, body))
        t.start()
        while True:
            t.join(self.heartbeat / 4)
            if t.is_alive():
                self.process_event_data()
            else:
                break
        self._threads.append(t)

    def work_function_wrapper(self, connection, channel, basic_deliver, properties, body):
        try:
            self.logger.debug('Starting work function of message # {}'.format(basic_deliver.delivery_tag))
            self.work_function(connection, channel, basic_deliver, properties, body)
            self.logger.debug('Finishing work function of message # {}'.format(basic_deliver.delivery_tag))
        except Exception as e:
            self.logger.error(str(e))
            self.logger.error(str(traceback.format_exc()))
            # raise e
        finally:
            self.logger.info('Acknowledging message {}'.format(basic_deliver.delivery_tag))
            self.acknowledge_message_callback(basic_deliver.delivery_tag)
