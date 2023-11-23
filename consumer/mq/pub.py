import abc
import functools
import json
import logging
import threading
import time
from io import BytesIO
from typing import Dict

import pika

from .base import MQBase


class MQPub(MQBase):
    def __init__(self, hostname: str, port: int, log_level: int = 10):
        super().__init__(hostname, port, log_level)
        LOG_FORMAT = '%(levelname)s:%(asctime)s:%(message)s'

        logging.basicConfig(level=log_level, format=LOG_FORMAT)
        self.logger = logging.getLogger(__name__)

        self.hostname = hostname
        self.port = port

    def publish(self, file: BytesIO | None = None, pub_context=None):
        if pub_context is None:
            pub_context = {}

        conn, chan = self.get_connection_and_channel()
        try:
            if not file:
                chan.basic_publish(exchange="", routing_key=self.pub_queue, body=json.dumps(pub_context).encode())
            else:
                tmp_queue_name = self.declare_queue()
                pub_context["queue"] = tmp_queue_name
                self.logger.info(f"Created temp queue {tmp_queue_name}")
                print(f"Created temp queue {tmp_queue_name}")

                self.logger.info(f"[ ] Posting chunks on {tmp_queue_name}")
                print(f"[ ] Posting chunks on {tmp_queue_name}")

                # Publish tempfile in tmp queue
                while chunk := file.read(1024 * 64):
                    chan.basic_publish(exchange="", routing_key=tmp_queue_name, body=chunk)

                self.logger.info(f"[X] Posting chunks on {tmp_queue_name}")
                print(f"[X] Posting chunks on {tmp_queue_name}")

                # Publish queue name in scp_queue
                chan.basic_publish(exchange="", routing_key=self.pub_queue, body=json.dumps(pub_context).encode())

        except Exception as e:
            self.logger.error(str(e))
            raise e

        finally:
            self.close(conn, chan)


if __name__ == "__main__":
    pass
