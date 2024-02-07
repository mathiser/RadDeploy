import logging
import os
import signal

from DicomFlowLib.conf import load_configs
from DicomFlowLib.log import init_logger
from DicomFlowLib.mq import PubModel, SubModel

from DicomFlowLib.fs import FileStorageClient
from DicomFlowLib.mq import MQSub
from docker_consumer.impl import DockerConsumer


class Main:
    def __init__(self, config):
        signal.signal(signal.SIGTERM, self.stop)

        self.running = None
        init_logger(name=config["LOG_NAME"],
                                         log_format=config["LOG_FORMAT"],
                                         log_dir=config["LOG_DIR"],
                                         rabbit_hostname=config["RABBIT_HOSTNAME"],
                                         rabbit_port=int(config["RABBIT_PORT"]),
                                         pub_models=[PubModel(**d) for d in config["LOG_PUB_MODELS"]])
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(int(config["LOG_LEVEL"]))

        self.fs = FileStorageClient(file_storage_url=config["FILE_STORAGE_URL"])

        self.ss = FileStorageClient(file_storage_url=config["STATIC_STORAGE_URL"],
                                    local_cache=config["STATIC_STORAGE_CACHE_DIR"])

        self.consumer = DockerConsumer(file_storage=self.fs,
                                       static_storage=self.ss,
                                       gpus=config["GPUS"],
                                       pub_routing_key_success=config["PUB_ROUTING_KEY_SUCCESS"],
                                       pub_routing_key_fail=config["PUB_ROUTING_KEY_FAIL"])

        if len(self.consumer.gpus) > 0:
            self.logger.info("GPUS are set. Changing sub_model routing_key to listen for GPU jobs only")
            sub_models = config["GPU_SUB_MODELS"]
            sub_queue_kwargs = config["GPU_SUB_QUEUE_KWARGS"]
        else:
            sub_models = config["CPU_SUB_MODELS"]
            sub_queue_kwargs = config["CPU_SUB_QUEUE_KWARGS"]

        self.mq = MQSub(rabbit_hostname=config["RABBIT_HOSTNAME"], rabbit_port=int(config["RABBIT_PORT"]),
                        sub_models=[SubModel(**sm) for sm in sub_models],
                        pub_models=[PubModel(**d) for d in config["PUB_MODELS"]],
                        work_function=self.consumer.mq_entrypoint, sub_prefetch_value=int(config["SUB_PREFETCH_COUNT"]),
                        sub_queue_kwargs=sub_queue_kwargs, pub_routing_key_error=config["PUB_ROUTING_KEY_ERROR"])

    def start(self):
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


if __name__ == "__main__":
    config = load_configs(os.environ["CONF_DIR"], os.environ["CURRENT_CONF"])

    m = Main(config=config)
    m.start()
