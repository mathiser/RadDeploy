import logging
import os
import queue

from typing import List

from DicomFlowLib.data_structures.contexts import PublishContext
from DicomFlowLib.mq import PubModel, MQPub


class CollectiveLogger:
    def __init__(self,
                 name: str,
                 log_dir: str,
                 log_level: int,
                 log_format: str,
                 pub_models: List[PubModel] | None = None,
                 rabbit_hostname: str | None = None,
                 rabbit_port: int | None = None):

        self.stopping = False
        self.logger = logging.getLogger(name=name)
        self.queue = queue.Queue()

        self.logger.setLevel(level=log_level)

        formatter = logging.Formatter(log_format)
        self.stream_handler = logging.StreamHandler()
        self.stream_handler.setFormatter(formatter)
        self.logger.addHandler(self.stream_handler)

        self.log_dir = log_dir
        os.makedirs(self.log_dir, exist_ok=True)

        self.log_file = os.path.join(self.log_dir, name + ".log")

        self.file_handler = logging.FileHandler(self.log_file, mode="a")
        self.file_handler.setFormatter(formatter)
        self.logger.addHandler(self.file_handler)

        if not pub_models:
            self.pub_models = []
        else:
            self.pub_models = pub_models

        self.mq = None
        if rabbit_hostname and rabbit_port:
            self.mq = MQPub(rabbit_hostname=rabbit_hostname,
                            rabbit_port=rabbit_port,
                            logger=self.logger)
            self.mq.connect()
            for pub_model in self.pub_models:
                self.mq.setup_exchange(pub_model.exchange, exchange_type=pub_model.exchange_type)
            self.mq.start()


    def process_log_call(self, queue_obj):
        level, uid, finished, msg = queue_obj
        if not finished:
            msg = f"UID:{uid} ; [ ] {msg} ; "
        else:
            msg = f"UID:{uid} ; [X] {msg} ; "
        return {"level": level,
                "msg": msg}

    def log(self, log_obj):
        self.logger.log(level=log_obj["level"], msg=log_obj["msg"])
        if self.mq:
            for pub_model in self.pub_models:
                pub_context = PublishContext(routing_key=str(log_obj["level"]),
                                             body=log_obj["msg"].encode())


                self.mq.add_publish_message(pub_model, pub_context)

    def stop(self, signalnum=None, stack_frame=None):
        self.stopping = True
        if self.mq:
            self.mq.stop()

    def debug(self, msg, uid: str = "SYSTEM", finished: bool = True):
        obj = self.process_log_call((10, uid, finished, msg))
        self.log(obj)

    def info(self, msg, uid: str = "SYSTEM", finished: bool = True):
        obj = self.process_log_call((20, uid, finished, msg))
        self.log(obj)

    def warning(self, msg, uid: str = "SYSTEM", finished: bool = True):
        obj = self.process_log_call((30, uid, finished, msg))
        self.log(obj)

    def error(self, msg, uid: str = "SYSTEM", finished: bool = True):
        obj = self.process_log_call((40, uid, finished, msg))
        self.log(obj)

    def critical(self, msg, uid: str = "SYSTEM", finished: bool = True):
        obj = self.process_log_call((50, uid, finished, msg))
        self.log(obj)