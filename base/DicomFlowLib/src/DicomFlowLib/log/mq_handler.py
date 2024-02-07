import json
import signal
from logging import StreamHandler
from typing import List
import socket

from DicomFlowLib.data_structures.contexts import PublishContext
from DicomFlowLib.mq import MQPub, PubModel


class MQHandler(StreamHandler):
    def __init__(self,
                 rabbit_hostname,
                 rabbit_port,
                 pub_models: List[PubModel],
                 ):
        signal.signal(signal.SIGTERM, self.stop)

        super().__init__()
        self.mq = MQPub(rabbit_hostname=rabbit_hostname,
                        rabbit_port=rabbit_port)
        self.pub_models = pub_models
        self.mq.start()

    def emit(self, record):
        record = record.__dict__
        record["hostname"] = socket.gethostname()
        for pub_model in self.pub_models:
            self.mq.add_publish_message(pub_model,
                                        PublishContext(body=json.dumps(record),
                                                       routing_key=f"{record["hostname"]}.{record["levelname"]}"))

    def stop(self, signalnum=None, stack_frame=None):
        self.mq.stop()
        self.mq.join()
