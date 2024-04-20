import logging
import signal
import threading
import time
from typing import List, Dict

from DicomFlowLib.fs.client.interface import FileStorageClientInterface
from DicomFlowLib.log import init_logger
from DicomFlowLib.log.mq_handler import MQHandler
from DicomFlowLib.mq import MQSub
from DicomFlowLib.mq import PubModel, SubModel
from docker_executor import DockerExecutor


class Consumer(threading.Thread):
    def __init__(self,
                 rabbit_hostname: str,
                 rabbit_port: int,
                 log_pub_models: List[PubModel],
                 log_format: str,
                 log_dir: str,
                 log_level: int,
                 pub_models: List[PubModel],
                 sub_models: List[SubModel],
                 worker_type: str,
                 worker_device_id: str,
                 file_storage: FileStorageClientInterface,
                 static_storage: FileStorageClientInterface,
                 sub_prefetch_value: int,
                 sub_queue_kwargs: Dict,
                 job_log_dir: str):
        super().__init__()
        signal.signal(signal.SIGTERM, self.stop)
        assert worker_type in ["CPU", "GPU"]

        self.running = False
        self.rabbit_hostname = rabbit_hostname
        self.rabbit_port = rabbit_port
        self.log_pub_models = log_pub_models
        self.log_format = log_format
        self.log_level = log_level
        self.pub_models = pub_models
        self.sub_models = sub_models
        self.worker_type = worker_type
        self.worker_device_id = worker_device_id
        self.file_storage = file_storage
        self.static_storage = static_storage
        self.sub_prefetch_value = sub_prefetch_value
        self.sub_queue_kwargs = sub_queue_kwargs
        self.job_log_dir = job_log_dir

        self.mq_handler = MQHandler(
            rabbit_hostname=rabbit_hostname,
            rabbit_port=rabbit_port,
            pub_models=log_pub_models
        )

        init_logger(name=None,  # init root logger,
                    log_format=log_format,
                    log_dir=log_dir,
                    mq_handler=self.mq_handler)

        logger = logging.getLogger(__name__)
        logger.setLevel(log_level)

        self.consumer = DockerExecutor(file_storage=file_storage,
                                       static_storage=static_storage,
                                       worker_device_id=worker_device_id,
                                       worker_type=worker_type,
                                       log_level=log_level,
                                       job_log_dir=job_log_dir)

        self.mq = MQSub(rabbit_hostname=rabbit_hostname,
                        rabbit_port=rabbit_port,
                        sub_models=sub_models,
                        pub_models=pub_models,
                        work_function=self.consumer.mq_entrypoint,
                        sub_prefetch_value=sub_prefetch_value,
                        sub_queue_kwargs=sub_queue_kwargs,
                        log_level=log_level)

    def run(self, blocking=True):
        self.running = True
        self.mq.start()
        self.mq_handler.start()

        while self.running and blocking:
            time.sleep(1)

    def stop(self, signalnum=None, stack_frame=None):
        self.running = False
        self.mq.stop()
        self.mq_handler.stop()
        self.mq.join()
        self.mq_handler.join()
