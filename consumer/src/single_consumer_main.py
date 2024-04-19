import logging
import signal
import time
from typing import List, Dict

from DicomFlowLib.fs.client.interface import FileStorageClientInterface
from DicomFlowLib.log import init_logger
from DicomFlowLib.log.mq_handler import MQHandler
from DicomFlowLib.mq import MQSub
from DicomFlowLib.mq import PubModel, SubModel
from docker_executor import Worker, DockerExecutor


class Consumer:
    def __init__(self,
                 rabbit_hostname: str,
                 rabbit_port: int,
                 log_pub_models: List[PubModel],
                 log_format: str,
                 log_dir: str,
                 log_level: int,
                 pub_models: List[PubModel],
                 sub_models: List[SubModel],
                 worker: Worker,
                 file_storage: FileStorageClientInterface,
                 static_storage: FileStorageClientInterface,
                 sub_prefetch_value: int,
                 sub_queue_kwargs: Dict):
        super().__init__()
        signal.signal(signal.SIGTERM, self.stop)

        self.running = False

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
                                       worker=worker,
                                       log_level=log_level)

        self.mq = MQSub(rabbit_hostname=rabbit_hostname,
                        rabbit_port=rabbit_port,
                        sub_models=sub_models,
                        pub_models=pub_models,
                        work_function=self.consumer.mq_entrypoint,
                        sub_prefetch_value=sub_prefetch_value,
                        sub_queue_kwargs=sub_queue_kwargs,
                        log_level=log_level)

    def start(self, blocking=False):
        self.running = True
        self.mq.start()
        self.mq_handler.start()

        while self.running and blocking:
            time.sleep(1)

        return self

    def stop(self, signalnum=None, stack_frame=None):
        self.running = False
        self.mq.stop()
        self.mq_handler.stop()
        self.mq.join()
        self.mq_handler.join()

