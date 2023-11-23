import atexit
import functools
import json
import threading

from .sub_file import MQSubFile
from .base import MQBase


class MQSub(MQBase):
    def __init__(self, hostname: str, port: int, log_level: int, queue_name: str, prefetch_value: int):
        super().__init__(hostname, port, log_level)
        self.connection, self.channel = self.get_connection_and_channel()
        self.prefetch_value = prefetch_value
        self.log_level = log_level
        self.threads = []
        self.queue_name = queue_name

        exit_func = functools.partial(self.on_exit)
        atexit.register(exit_func)

    def subscribe(self):
        q = self.declare_queue(self.queue_name)
        print(q)
        self.logger.info(f": Setting RabbitMQ prefetch_count to {self.prefetch_value}")
        self.channel.basic_qos(prefetch_count=self.prefetch_value)

        on_message_callback = functools.partial(self.on_message, args=(self.connection, self.threads))
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=on_message_callback)

        self.logger.info(' [*] Waiting for messages')
        self.channel.start_consuming()


    def on_message(self, channel, method_frame, header_frame, body, args):
        (connection, threads) = args
        delivery_tag = method_frame.delivery_tag

        t = threading.Thread(target=self.do_work, args=(self.connection, channel, delivery_tag, body))
        t.start()
        threads.append(t)

    def ack_message(self, channel, delivery_tag):
        if self.channel.is_open:
            channel.basic_ack(delivery_tag)
        else:
            raise Exception("Channel closed - Y tho?")

    def on_exit(self):
        for thread in self.threads:
            thread.join()
        if self.connection.is_open:
            self.connection.close()
        if self.channel.is_open:
            self.channel.close()

    def do_work(self, connection, channel, delivery_tag, body):
        meta = json.loads(body.decode())

        mq_file = MQSubFile(hostname=self.hostname, port=self.port, log_level=self.log_level)
        file = mq_file.consume_queue_to_file(meta=meta)
        print(meta["checksum"])
        cb = functools.partial(self.ack_message, channel, delivery_tag)
        connection.add_callback_threadsafe(cb)
        #self.channel.queue_delete(meta["queue"], if_empty=True)


if __name__ == "__main__":
    pass
