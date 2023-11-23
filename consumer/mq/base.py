import logging
import time

import pika

LOG_FORMAT = '%(levelname)s:%(asctime)s:%(message)s'


class MQBase:
    def __init__(self,
                 hostname: str,
                 port: int,
                 log_level: int = 10):

        logging.basicConfig(level=log_level, format=LOG_FORMAT)
        self.logger = logging.getLogger(__name__)

        self.hostname = hostname
        self.port = port

    def declare_queue(self, queue_name: str = ""):
        conn, chan = self.get_connection_and_channel()
        try:
            return chan.queue_declare(queue=queue_name, durable=True)

        except Exception as e:
            self.logger.error(
                f"Could not connect to RabbitMQ - is it running? Expecting it on {self.hostname}:{self.port}")
        finally:
            self.close(conn, chan)

    def get_connection_and_channel(self):
        while True:
            try:
                connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.hostname,
                                                                               port=self.port))
                channel = connection.channel()
                if channel.is_open:
                    return connection, channel
            except Exception as e:
                self.logger.error(
                    f"Could not connect to RabbitMQ - is it running? Expecting it on {self.hostname}:{self.port}")
                time.sleep(10)

    def close(self, connection, channel):
        if channel.is_open:
            channel.close()
        if connection.is_open:
            connection.close()



if __name__ == "__main__":
    pass
