# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205
import threading
import traceback
from typing import List, Dict

from .base import MQBase
from ..data_structures.contexts import PublishContext
from .mq_models import PubModel, SubModel


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
                 pub_models: List[PubModel],
                 work_function: callable,
                 sub_prefetch_value: int,
                 logger,
                 sub_queue_kwargs: Dict,
                 pub_routing_key_error: str):

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

        self.sub_models = sub_models
        self.pub_models = pub_models
        self.pub_routing_key_error = pub_routing_key_error
        self.sub_queue_kwargs = sub_queue_kwargs
        self.queue = None

    def run(self):
        # Set Connection and channel
        self.connect()
        self.queue = self.setup_queue(**self.sub_queue_kwargs)

        # Declare exchange
        for sub in self.sub_models:
            self.setup_exchange(exchange=sub.exchange, exchange_type=sub.exchange_type)

            for rk in sub.routing_keys:
                # Declare queue
                self.bind_queue(queue=self.queue, exchange=sub.exchange, routing_key=rk)

            self.logger.debug(f"Setting up sub model {sub}", finished=True)
        # Set prefetch value
        self._channel.basic_qos(prefetch_count=self.sub_prefetch_value)

        # Set consuming queue
        self._channel.basic_consume(queue=self.queue, on_message_callback=self.on_message)

        self.logger.info(' [*] Waiting for messages')
        self._channel.start_consuming()

    def fetch_echo(self, basic_deliver, body):
        for sub_model in self.sub_models:
            if sub_model.exchange == basic_deliver.exchange and sub_model.routing_key == basic_deliver.routing_key:
                self.basic_publish_callback(exchange=sub_model.exchange,
                                            routing_key=sub_model.routing_key_fetch_echo,
                                            body=body)
            self.process_event_data()

    def publish_on_all_pub_models(self,
                                  result: PublishContext):
        for pub_model in self.pub_models:
            self.logger.debug(f"publishing on exchange: {pub_model.exchange} with routing key: {result.routing_key}")

            self.basic_publish_callback(exchange=pub_model.exchange,
                                        routing_key=result.routing_key,
                                        body=result.body,
                                        priority=result.priority)

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        self.logger.debug('Received message # {} from {}'.format(basic_deliver.delivery_tag, properties.app_id))

        self.fetch_echo(basic_deliver=basic_deliver, body=body)

        t = threading.Thread(target=self.work_function_wrapper,
                             args=(basic_deliver, body))
        t.start()

    def work_function_wrapper(self, basic_deliver, body):
        try:
            result: PublishContext
            for result in self.work_function(basic_deliver, body):
                self.publish_on_all_pub_models(result=result)  # Routing key on success

        except Exception as e:
            self.logger.error(str(e))
            self.logger.error(str(traceback.format_exc()))
            self.publish_on_all_pub_models(
                result=PublishContext(body=body, routing_key=self.pub_routing_key_error))  # routing key on fail
        finally:
            self.logger.debug('Acknowledging message {}'.format(basic_deliver.delivery_tag))
            self.acknowledge_message_callback(basic_deliver.delivery_tag)
