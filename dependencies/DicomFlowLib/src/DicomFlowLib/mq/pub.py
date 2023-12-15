import logging
import queue
import traceback
from queue import Queue

from pydantic import BaseModel

from DicomFlowLib.default_config import LOG_FORMAT
from .base import MQBase


class MQPub(MQBase):
    def __init__(self, hostname: str, port: int, log_level: int,
                 publish_interval: int, publish_queue: Queue,
                 routing_key: str, routing_key_as_queue: bool = True,
                 exchange: str = "", exchange_type: str = "direct",
                 ):

        super().__init__(hostname, port, log_level)

        logging.basicConfig(level=log_level, format=LOG_FORMAT)
        self.LOGGER = logging.getLogger(__name__)

        self.publish_interval = publish_interval
        self.publish_queue = publish_queue
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.routing_key = routing_key
        self.routing_key_as_queue = routing_key_as_queue

        self._deliveries = {}
        self._acked = 0
        self._nacked = 0
        self._message_number = 0

        self.connect()
        self.setup_exchange(exchange=self.exchange,
                            exchange_type=self.exchange_type)

        self.setup_queue(exchange=self.exchange,
                         routing_key=routing_key,
                         routing_key_as_queue=routing_key_as_queue)

    def publish_message(self, context):

        self._channel.basic_publish(exchange=self.exchange,
                                    routing_key=self.routing_key,
                                    body=context.to_json())

        self._message_number += 1
        self._deliveries[self._message_number] = True
        self.LOGGER.info('Published message # %i', self._message_number)

    def run(self):
        """Run the example code by connecting and then starting the IOLoop.
        """

        while not self._stopping:
            self._deliveries = {}
            self._acked = 0
            self._nacked = 0
            self._message_number = 0

            self.LOGGER.info('Made connection')
            self.connect()

            # self.LOGGER.info('Enabling delivery confirmations')
            # self.enable_delivery_confirmations()

            self.LOGGER.info('Starting publishing')
            while not self._stopping:
                try:
                    context = self.publish_queue.get(timeout=30)
                    self.publish_message(context=context)
                except queue.Empty:
                    self._connection.process_data_events()

                except KeyboardInterrupt:
                    self.stop()
                except Exception as e:
                    self.LOGGER.error(str(e))
                    self.LOGGER.error(traceback.format_exc())
                    raise e

        self.LOGGER.info('Stopped')

    def enable_delivery_confirmations(self):
        """Send the Confirm.Select RPC method to RabbitMQ to enable delivery
        confirmations on the channel. The only way to turn this off is to close
        the channel and create a new one.

        When the message is confirmed from RabbitMQ, the
        on_delivery_confirmation method will be invoked passing in a Basic.Ack
        or Basic.Nack method from RabbitMQ that will indicate which messages it
        is confirming or rejecting.

        """
        self.LOGGER.info('Issuing Confirm.Select RPC command')
        self._channel.confirm_delivery(self.on_delivery_confirmation)

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

        self.LOGGER.info('Received %s for delivery tag: %i (multiple: %s)',
                         confirmation_type, delivery_tag, ack_multiple)

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

        self.LOGGER.info(
            'Published %i messages, %i have yet to be confirmed, '
            '%i were acked and %i were nacked', self._message_number,
            len(self._deliveries), self._acked, self._nacked)