import json
import signal
from logging import StreamHandler
from typing import List
import socket

from RadDeployLib.mq.mq_models import PublishContext
from RadDeployLib.mq import MQPub, PubModel


class MQHandler(StreamHandler):
    def __init__(self,
                 rabbit_hostname,
                 rabbit_port,
                 pub_models: List[PubModel],
                 ):
        super().__init__()
        signal.signal(signal.SIGTERM, self.stop)
        self.mq = MQPub(rabbit_hostname=rabbit_hostname,
                        rabbit_port=rabbit_port,
                        log_level=0)
        self.pub_models = pub_models
        self.running = False

    def emit(self, record):
        if self.running:
            record = record.__dict__
            record["hostname"] = socket.gethostname()
            for pub_model in self.pub_models:
                pub_model.routing_key_values[record["levelname"]] = f'{record["hostname"]}.{record["levelname"]}'
                self.mq.add_publish_message(
                    pub_model,
                    PublishContext(body=json.dumps(record),
                                   pub_model_routing_key=record["levelname"])
                )

    def start(self):
        self.running = True
        self.mq.start()


    def stop(self, signalnum=None, stack_frame=None):
        self.running = False
        self.mq.stop()


    def join(self):
        self.mq.join()