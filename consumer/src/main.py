import logging
import os
import signal
import sys

from DicomFlowLib.conf import load_configs
from DicomFlowLib.fs import FileStorageClient
from DicomFlowLib.log import init_logger
from DicomFlowLib.mq import MQSub
from DicomFlowLib.mq import PubModel, SubModel
from docker_consumer import Worker, DockerConsumer


class MainConsumer:
    def __init__(self, config, worker: Worker, sub_models, sub_models_kwargs):
        signal.signal(signal.SIGTERM, self.stop)
        self.worker = worker
        self.running = None
        init_logger(name=config["LOG_NAME"],
                    log_format=config["LOG_FORMAT"],
                    log_dir=config["LOG_DIR"],
                    rabbit_hostname=config["RABBIT_HOSTNAME"],
                    rabbit_port=int(config["RABBIT_PORT"]),
                    pub_models=[PubModel(**d) for d in config["LOG_PUB_MODELS"]])
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(int(config["LOG_LEVEL"]))
        self.sub_models = sub_models
        self.sub_models_kwargs = sub_models_kwargs
        self.fs = FileStorageClient(file_storage_url=config["FILE_STORAGE_URL"],
                                    log_level=int(config["LOG_LEVEL"]))

        self.ss = FileStorageClient(file_storage_url=config["STATIC_STORAGE_URL"],
                                    local_cache=config["STATIC_STORAGE_CACHE_DIR"],
                                    log_level=int(config["LOG_LEVEL"])
                                    )

        self.consumer = DockerConsumer(file_storage=self.fs,
                                       static_storage=self.ss,
                                       worker=self.worker,
                                       pub_routing_key_success=config["PUB_ROUTING_KEY_SUCCESS"],
                                       pub_routing_key_fail=config["PUB_ROUTING_KEY_FAIL"],
                                       log_level=int(config["LOG_LEVEL"], ))

        self.mq = MQSub(rabbit_hostname=config["RABBIT_HOSTNAME"], rabbit_port=int(config["RABBIT_PORT"]),
                        sub_models=[SubModel(**sm) for sm in self.sub_models],
                        pub_models=[PubModel(**d) for d in config["PUB_MODELS"]],
                        work_function=self.consumer.mq_entrypoint, sub_prefetch_value=int(config["SUB_PREFETCH_COUNT"]),
                        sub_queue_kwargs=self.sub_models_kwargs, pub_routing_key_error=config["PUB_ROUTING_KEY_ERROR"],
                        log_level=int(config["LOG_LEVEL"]))
        self.start()

    def start(self):
        if self.running:
            return

        self.running = True

        self.mq.start()
        while self.running:
            try:
                self.mq.join(timeout=5)
                if self.mq.is_alive():
                    pass
                else:
                    self.stop()
            except KeyboardInterrupt:
                self.stop()

    def stop(self, signalnum=None, stack_frame=None):
        self.running = False
        self.mq.stop()
        self.mq.join()
