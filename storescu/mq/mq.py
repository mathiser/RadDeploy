import json
import logging
import time

import pika

from db.models import Flow, MQConfig


class MQ:
    def __init__(self, mq_config: MQConfig, log_level: int = 10 ):
        LOG_FORMAT = '%(levelname)s:%(asctime)s:%(message)s'

        logging.basicConfig(level=log_level, format=LOG_FORMAT)
        self.logger = logging.getLogger(__name__)

        self.mq_config = mq_config

    def get_connection_and_channel(self):
        while True:
            try:
                connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.mq_config.host, port=self.mq_config.port))
                channel = connection.channel()
                if channel.is_open:
                    channel.queue_declare(queue=self.mq_config.scheduled_flow_queue, durable=True)
                    return connection, channel

            except Exception as e:
                self.logger.error(f"Could not connect to RabbitMQ - is it running? Expecting it on {self.mq_config.host}:{self.mq_config.port}")
                time.sleep(10)

    def close(self, connection, channel):
        if channel.is_open:
            channel.close()
        if connection.is_open:
            connection.close()

    def schedule_flow(self, flow: Flow):
        conn, chan = self.get_connection_and_channel()
        flow_dict = json.dumps(flow.__dict__)
        try:
            if flow.gpu:
                chan.basic_publish(exchange="", routing_key=self.mq_config.scheduled_flow_gpu, body=flow_dict.encode())
            else:
                chan.basic_publish(exchange="", routing_key=self.mq_config.scheduled_flow_cpu, body=flow_dict.encode())
        except Exception as e:
            self.logger.error(e)
            raise e

        self.close(conn, chan)


if __name__ == "__main__":
    pass