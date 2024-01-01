import functools
import threading

import pika
from pika import channel, connection

from DicomFlowLib.log.logger import CollectiveLogger


class MQBase(threading.Thread):
    def __init__(self,
                 logger: CollectiveLogger,
                 hostname: str | None = None,
                 port: int | None = None):
        super().__init__()
        self.logger = logger
        self._hostname = hostname
        self._port = port
        self._connection = None
        self._channel = None

        self._stopping = False

        self._declared_exchanges = {""}
        self._declared_queues = set()

    def connect_like(self, connection: pika.connection.Connection):
        self.logger.info('Connecting to {}:{}'.format(self._hostname, self._port))
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=connection._impl.params.host, port=connection._impl.params.port)
        )
        self._channel = self._connection.channel()
        return self

    def connect_with(self,
                     connection: connection.Connection,
                     channel: channel.Channel | None = None):
        if connection and channel:
            self.logger.info('Using existing connection and connection')
            self._connection = connection
            self._channel = channel
        elif connection and not channel:
            self.logger.info('Using existing connection - opening new channel')
            self._connection = connection
            self._channel = self._connection.channel()
        return self

    def connect(self):
        assert self._hostname and self._port
        self.logger.info('Connecting to {}:{}'.format(self._hostname, self._port))
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._hostname, port=self._port))
        self._channel = self._connection.channel()

        return self

    def stop(self):
        """Stop the example by closing the channel and connection. We
        set a flag here so that we stop scheduling new messages to be
        published. The IOLoop is started because this method is
        invoked by the Try/Catch below when KeyboardInterrupt is caught.
        Starting the IOLoop again will allow the publisher to cleanly
        disconnect from RabbitMQ.

        """
        self.logger.debug('Stopping')
        self._stopping = True
        self.close_channel()
        self.close_connection()

    def close_channel(self):
        """Invoke this command to close the channel with RabbitMQ by sending
        the Channel.Close RPC command.

        """
        if self._channel is not None:
            if self._channel.is_open:
                self.logger.info('Closing the channel')
                self._channel.close()

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        if self._connection is not None:
            if self._connection.is_open:
                self.logger.info('Closing connection')
                self._connection.close()

    def setup_exchange_callback(self, exchange: str, exchange_type: str = "direct"):
        cb = functools.partial(self.setup_exchange, exchange=exchange, exchange_type=exchange_type)
        self._connection.add_callback_threadsafe(cb)

    def setup_exchange(self, exchange: str, exchange_type: str = "direct"):
        if exchange in self._declared_exchanges:
            self.logger.debug('Exchange {} type %s already exist'.format(exchange, exchange_type))
        else:
            self.logger.info('Declaring exchange %s type %s'.format(exchange, exchange_type))
            self._channel.exchange_declare(exchange=exchange,
                                           exchange_type=exchange_type)
            self._declared_exchanges.add(exchange)

    def setup_queue_callback(self, queue: str):
        cb = functools.partial(self.setup_queue, queue=queue)
        self._connection.add_callback_threadsafe(cb)

    def setup_queue(self, queue: str):
        queue = self._channel.queue_declare(queue=queue, arguments={"x-max-priority": 5}).method.queue
        self._declared_queues.add(queue)
        return queue

    def setup_queue_and_bind_callback(self, routing_key: str, routing_key_as_queue: bool = False, exchange: str = ""):
        cb = functools.partial(self.setup_queue_and_bind, exchange=exchange, routing_key=routing_key,
                               routing_key_as_queue=routing_key_as_queue)
        self._connection.add_callback_threadsafe(cb)

    def setup_queue_and_bind(self, exchange: str, routing_key: str, routing_key_as_queue: bool = False):
        if exchange not in self._declared_exchanges:
            raise Exception("Exchange is not declared")

        if routing_key_as_queue:
            queue = routing_key
            routing_key = None
        else:
            queue = ""
            routing_key = routing_key

        if queue not in self._declared_queues:
            queue = self.setup_queue(queue)
            self.bind_queue(queue=queue, exchange=exchange, routing_key=routing_key)
            self.logger.info('Declaring queue %s on exchange %s'.format(queue, exchange))
            self._declared_queues.add(queue)
        return queue

    def bind_queue_callback(self, exchange: str, routing_key: str, queue: str):
        cb = functools.partial(self.bind_queue, exchange=exchange, routing_key=routing_key, queue=queue)
        self._connection.add_callback_threadsafe(cb)

    def bind_queue(self, queue: str, exchange: str, routing_key: str):
        if exchange == "":
            self.logger.debug("Binding queues on default exchange is not allowed - continuing")
        else:
            self.logger.info("Binding queue: %s".format(queue))
            self._channel.queue_bind(queue=queue, exchange=exchange, routing_key=routing_key)
            self._declared_queues.add(queue)

    def acknowledge_message_callback(self, delivery_tag):
        cb = functools.partial(self.acknowledge_message, delivery_tag=delivery_tag)
        self._connection.add_callback_threadsafe(cb)

    def acknowledge_message(self, delivery_tag):
        if self._channel.is_open:
            self._channel.basic_ack(delivery_tag)
        else:
            raise Exception("Channel closed - Y tho?")

    def basic_publish_callback(self, exchange: str, routing_key: str, body: bytes, priority: int = 0,
                               reply_to: str | None = None):
        cb = functools.partial(self.basic_publish,
                               exchange=exchange,
                               routing_key=routing_key,
                               body=body,
                               priority=priority,
                               reply_to=reply_to)
        self._connection.add_callback_threadsafe(cb)

    def basic_publish(self, exchange: str, routing_key: str, body: bytes, priority: int = 0,
                      reply_to: str | None = None):
        self.logger.info(f"Publishing with routing_key: {routing_key} on exchange: {exchange}", finished=False)

        self._channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=body,
            properties=pika.BasicProperties(
                reply_to=reply_to,
                priority=priority
            )
        )
        self.logger.info(f"Publishing with routing_key: {routing_key} on exchange: {exchange}", finished=True)

    def delete_queue_callback(self, queue: str, if_empty: bool = False):
        cb = functools.partial(self.delete_queue,
                               queue=queue,
                               if_empty=if_empty)
        self._connection.add_callback_threadsafe(cb)

    def delete_queue(self, queue, if_empty: bool = False):
        self._channel.queue_delete(queue=queue, if_empty=if_empty)

    def process_event_data(self):
        self.logger.debug("Processing event data")
        self._connection.process_data_events()

    def process_event_data_callback(self):
        cb = functools.partial(self.process_event_data)
        self._connection.add_callback_threadsafe(cb)
