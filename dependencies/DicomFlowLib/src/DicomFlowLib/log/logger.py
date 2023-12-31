import logging
import os
import queue
import threading

from python_logging_rabbitmq import RabbitMQHandler

LOG_FORMAT = ('%(levelname)s ; %(asctime)s ; %(name)s ; %(filename)s ; %(funcName)s ; %(lineno)s ; %(message)s')


class CollectiveLogger(threading.Thread):
    def __init__(self,
                 name: str,
                 log_file: str | None = None,
                 log_level: int = 20,
                 log_format: str = LOG_FORMAT,
                 rabbit_host: str | None = "localhost",
                 rabbit_port: int | None = 5672,
                 rabbit_username: str = "guest",
                 rabbit_password: str = "guest"):
        super().__init__()
        self.stopping = False
        self.logger = logging.getLogger(name=name)
        self.queue = queue.Queue()

        self.logger.setLevel(level=log_level)

        formatter = logging.Formatter(log_format)
        self.stream_handler = logging.StreamHandler()
        self.stream_handler.setFormatter(formatter)
        self.logger.addHandler(self.stream_handler)

        if not log_file:
            log_file = os.path.join("logs", name + ".log")
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        self.file_handler = logging.FileHandler(log_file, mode="a")
        self.file_handler.setFormatter(formatter)
        self.logger.addHandler(self.file_handler)

        self.mq_handler = None
        if rabbit_host and rabbit_port:
            self.mq_handler = RabbitMQHandler(host=rabbit_host,
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
            timer = 0
            inteval = 10
            while timer <= (self.mq_handler.connection_params["heartbeat"] / 2):
                try:
                    timer += inteval
                    log_obj = self.queue.get(timeout=inteval)
                    log_obj = self.process_log_call(log_obj)
                    self.logger.log(**log_obj)
                except queue.Empty:
                    if self.mq_handler:
                        self.mq_handler.connection.process_data_events()

                except KeyboardInterrupt:
                    self.stopping = True
            if self.mq_handler:
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
