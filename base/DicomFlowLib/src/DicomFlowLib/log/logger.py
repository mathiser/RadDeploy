import logging
import os
import queue
import threading
import time

from python_logging_rabbitmq import RabbitMQHandler


class CollectiveLogger(threading.Thread):
    def __init__(self,
                 name: str,
                 log_dir: str,
                 log_level: int,
                 log_format: str,
                 rabbit_hostname: str | None = None,
                 rabbit_port: int | None = None,
                 rabbit_username: str | None = None,
                 rabbit_password: str | None = None):
        super().__init__()
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

        self.mq_handler = None
        if rabbit_hostname and rabbit_port and rabbit_username and rabbit_password:
            self.mq_handler = RabbitMQHandler(host=rabbit_hostname,
                                              port=rabbit_port,
                                              username=rabbit_username,
                                              password=rabbit_password,
                                              declare_exchange=True)
            self.logger.addHandler(self.mq_handler)

    def process_log_call(self, queue_obj):
        level, uid, finished, msg = queue_obj
        if not finished:
            msg = f"UID:{uid} ; [ ] {msg} ; "
        else:
            msg = f"UID:{uid} ; [X] {msg} ; "
        return {"level": level,
                "msg": msg}

    def run(self):
        while not self.stopping:
            time_zero = time.time()
            interval = 10
            while time.time() - time_zero < (self.mq_handler.connection_params["heartbeat"] / 4):
                try:
                    log_obj = self.queue.get(timeout=interval)
                    log_obj = self.process_log_call(log_obj)
                    self.logger.log(**log_obj)
                except queue.Empty:
                    if not self.stopping:
                        if self.mq_handler:
                            self.mq_handler.connection.process_data_events()
                except KeyboardInterrupt:
                    self.stopping = True
            if not self.stopping and self.mq_handler:
                self.mq_handler.connection.process_data_events()

    def stop(self):
        self.stopping = True
        self.mq_handler.close()

    def debug(self, msg, uid: str = "SYSTEM", finished: bool = True):
        self.queue.put((10, uid, finished, msg))

    def info(self, msg, uid: str = "SYSTEM", finished: bool = True):
        self.queue.put((20, uid, finished, msg))

    def warning(self, msg, uid: str = "SYSTEM", finished: bool = True):
        self.queue.put((30, uid, finished, msg))

    def error(self, msg, uid: str = "SYSTEM", finished: bool = True):
        self.queue.put((40, uid, finished, msg))

    def critical(self, msg, uid: str = "SYSTEM", finished: bool = True):
        self.queue.put((50, uid, finished, msg))
