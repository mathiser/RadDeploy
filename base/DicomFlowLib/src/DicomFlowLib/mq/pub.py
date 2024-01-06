import queue
import time
import traceback
from queue import Queue

from .base import MQBase
from ..data_structures.contexts import PublishContext
from ..log.logger import CollectiveLogger


class MQPub(MQBase):
    def     __init__(self,
                     rabbit_hostname: str,
                     rabbit_port: int,
                     publish_queue: Queue,
                     logger: CollectiveLogger):
        super().__init__(logger, rabbit_hostname, rabbit_port)

        self.publish_queue = publish_queue

        self._deliveries = {}
        self._acked = 0
        self._nacked = 0
        self._message_number = 0

    def publish_message(self, context: PublishContext):
        self.setup_exchange(exchange=context.exchange,
                            exchange_type=context.exchange_type)

        self.setup_queue_and_bind(exchange=context.exchange,
                                  routing_key=context.routing_key,
                                  routing_key_as_queue=context.routing_key_as_queue)

        self.basic_publish(exchange=context.exchange,
                           routing_key=context.routing_key,
                           body=context.body,
                           reply_to=context.reply_to,
                           priority=context.priority)

        self._message_number += 1
        self._deliveries[self._message_number] = True
        self.logger.info('Published message # {}'.format(self._message_number))

    def run(self):
        while not self._stopping:
            self._deliveries = {}
            self._acked = 0
            self._nacked = 0
            self._message_number = 0

            self.logger.debug("Connecting", finished=False)

            self.connect()
            self.logger.debug("Connecting", finished=True)
            # self.logger.info('Enabling delivery confirmations')
            # self.enable_delivery_confirmations()

            self.logger.info('Starting publishing')
            while not self._stopping:
                time_zero = time.time()
                interval = 10
                while time.time() - time_zero < (self.heartbeat / 2):
                    try:
                        context = self.publish_queue.get(timeout=interval)
                        self.publish_message(context=context)
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
