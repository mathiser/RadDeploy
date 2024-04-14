import functools
import queue
import time
import traceback
from typing import Tuple

from .mq_models import PubModel
from .base import MQBase
from ..data_structures.contexts import PublishContext


class MQPub(MQBase):
    def __init__(self,
                 in_queue: queue.Queue[Tuple[PubModel, PublishContext]],
                 rabbit_hostname: str,
                 rabbit_port: int,
                 log_level: int = 20):
        super().__init__(close_conn_on_exit=True,
                         rabbit_hostname=rabbit_hostname,
                         rabbit_port=rabbit_port,
                         log_level=log_level)

        self.in_queue = in_queue

        self._deliveries = {}
        self._acked = 0
        self._nacked = 0
        self._message_number = 0

    def __del__(self):
        self.stop()

    def add_publish_message(self, pub_model: PubModel, pub_context: PublishContext):
        self.in_queue.put((pub_model, pub_context))

    def publish_message_callback(self, pub_model: PubModel, pub_context: PublishContext):
        cb = functools.partial(self.publish_message, pub_model=pub_model, pub_context=pub_context)
        self._connection.add_callback_threadsafe(cb)

    def publish_message(self, pub_model: PubModel, pub_context: PublishContext):
        # Declare Exchanges
        self.setup_exchange(exchange=pub_model.exchange,
                            exchange_type=pub_model.exchange_type)

        self.basic_publish(exchange=pub_model.exchange,
                           routing_key=pub_context.routing_key,
                           body=pub_context.body,
                           reply_to=pub_context.reply_to,
                           priority=pub_context.priority)

        self._message_number += 1
        self._deliveries[self._message_number] = True


    def run(self):
        while not self._stopping:
            self._deliveries = {}
            self._acked = 0
            self._nacked = 0
            self._message_number = 0

            self.connect()
            self.logger.debug("Connected to RabbitMQ")
            # self.logger.info('Enabling delivery confirmations')
            # self.enable_delivery_confirmations()

            self.logger.info('Starting publishing')
            while not self._stopping:
                time_zero = time.time()
                interval = 10
                while time.time() - time_zero < (self.heartbeat / 4):
                    try:
                        pubmodel_publishcontext = self.in_queue.get(timeout=interval)
                        self.publish_message(*pubmodel_publishcontext)
                    except queue.Empty:
                        if not self._stopping and self._connection:
                            self.process_event_data()
                    except KeyboardInterrupt:
                        if not self._stopping:
                            self.stop()
                    except Exception as e:
                        self.logger.error(str(e))
                        self.logger.error(traceback.format_exc())
                        raise e
                    self.process_event_data()

                if not self._stopping and self._connection:
                    self.process_event_data()

        self.logger.info('Stopped')

    def enable_delivery_confirmations(self):
        """Send the Confirm.Select RPC method to RabbitMQ to enable delivery
        confirmations on the channel. The only way to turn this off is to close
        the channel and create a new one.

        When the message is confirmed from RabbitMQ, the
        on_delivery_confirmation method will be invoked passing in a Basic.Ack
        or Basic.Nack method from RabbitMQ that will indicate which messages it
        is confirming or rejecting.

        """

        self._channel.confirm_delivery(ack_nack_callback=self.on_delivery_confirmation)

    def on_delivery_confirmation(self, method_frame):
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
        command, passing in either a Basic.Ack or Basic.Nack frame with
        the delivery tag of the message that was published. The delivery tag
        is an integer counter indicating the message number that was sent
        on the channel via Basic.Publish. Here we're just doing house keeping
        to keep track of stats and remove message numbers that we expect
        a delivery confirmation of from the list used to keep track of messages
        that are pending confirmation.

        :param pika.frame.Method method_frame: Basic.Ack or Basic.Nack frame

        """
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        ack_multiple = method_frame.method.multiple
        delivery_tag = method_frame.method.delivery_tag

        self.logger.info('Received {} for delivery tag: {} (multiple: {})'.format(
            confirmation_type, delivery_tag, ack_multiple))

        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1

        del self._deliveries[delivery_tag]

        if ack_multiple:
            for tmp_tag in list(self._deliveries.keys()):
                if tmp_tag <= delivery_tag:
                    self._acked += 1
                    del self._deliveries[tmp_tag]
        """
        NOTE: at some point you would check self._deliveries for stale
        entries and decide to attempt re-delivery
        """

        self.logger.info(
            'Published {} messages, {} have yet to be confirmed, '
            '{} were acked and {} were nacked'.format(self._message_number,
                                                      len(self._deliveries), self._acked, self._nacked))
