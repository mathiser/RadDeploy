import logging
import os
import signal


from DicomFlowLib.conf import load_configs
from DicomFlowLib.fs import FileStorageClient
from DicomFlowLib.fs.client.interface import FileStorageClientInterface
from DicomFlowLib.log import init_logger
from DicomFlowLib.log.mq_handler import MQHandler
from DicomFlowLib.mq import PubModel, SubModel
from consumer import Consumer
import time


class Main:
    def __init__(self,
                 config,
                 file_storage: FileStorageClientInterface,
                 static_storage: FileStorageClientInterface):
        signal.signal(signal.SIGTERM, self.stop)
        self.running = False

        self.mq_handler = MQHandler(
            rabbit_hostname=config["RABBIT_HOSTNAME"],
            rabbit_port=int(config["RABBIT_PORT"]),
            pub_models=[PubModel(**d) for d in config["LOG_PUB_MODELS"]]
        )

        init_logger(name=None,  # init root logger,
                    log_format=config["LOG_FORMAT"],
                    log_dir=config["LOG_DIR"],
                    mq_handler=self.mq_handler)

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(int(config["LOG_LEVEL"]))

        self.file_storage = file_storage
        self.static_storage = static_storage

        self.workers = []
        cpus = int(config["CPUS"])
        for cpu_id in range(cpus):
            self.logger.info(f"Spawning CPU worker: {cpu_id}")
            m = Consumer(log_level=int(config["LOG_LEVEL"]),
                         worker_type="CPU",
                         worker_device_id=str(cpu_id),
                         rabbit_port=config["RABBIT_PORT"],
                         rabbit_hostname=config["RABBIT_HOSTNAME"],
                         file_storage=self.file_storage,
                         static_storage=self.static_storage,
                         log_dir=config["LOG_DIR"],
                         log_format=config["LOG_FORMAT"],
                         pub_models=[PubModel(**pm) for pm in config["PUB_MODELS"]],
                         sub_models=[SubModel(**sm) for sm in config["CPU_SUB_MODELS"]],
                         log_pub_models=[PubModel(**pm) for pm in config["LOG_PUB_MODELS"]],
                         sub_queue_kwargs=config["CPU_SUB_QUEUE_KWARGS"],
                         sub_prefetch_value=config["SUB_PREFETCH_COUNT"],
                         job_log_dir=config["JOB_LOG_DIR"])
            self.workers.append(m)

        if isinstance(config["GPUS"], str):
            device_ids = config["GPUS"].split()
        else:
            device_ids = config["GPUS"]

        for device_id in device_ids:
            self.logger.info(f"Spawning GPU worker: {device_id}")
            m = Consumer(log_level=int(config["LOG_LEVEL"]),
                         worker_type="GPU",
                         worker_device_id=str(device_id),
                         rabbit_port=config["RABBIT_PORT"],
                         rabbit_hostname=config["RABBIT_HOSTNAME"],
                         file_storage=self.file_storage,
                         static_storage=self.static_storage,
                         log_dir=config["LOG_DIR"],
                         log_format=config["LOG_FORMAT"],
                         pub_models=[PubModel(**pm) for pm in config["PUB_MODELS"]],
                         sub_models=[SubModel(**sm) for sm in config["GPU_SUB_MODELS"]],
                         log_pub_models=[PubModel(**pm) for pm in config["LOG_PUB_MODELS"]],
                         sub_queue_kwargs=config["GPU_SUB_QUEUE_KWARGS"],
                         sub_prefetch_value=config["SUB_PREFETCH_COUNT"],
                         job_log_dir=config["JOB_LOG_DIR"])
            self.workers.append(m)

    def start(self, blocking=True, threads_blocking=True):
        self.running = True
        for worker in self.workers:
            worker.start()

        while self.running and blocking:
            time.sleep(1)

    def stop(self, signalnum=None, stack_frame=None):
        self.running = False
        for worker in self.workers:
            worker.stop()

        for worker in self.workers:
            worker.join()


if __name__ == "__main__":
    config = load_configs(os.environ["CONF_DIR"], os.environ["CURRENT_CONF"])
    fs = FileStorageClient(file_storage_url=config["FILE_STORAGE_URL"],
                           log_level=int(config["LOG_LEVEL"]))

    ss = FileStorageClient(file_storage_url=config["STATIC_STORAGE_URL"],
                           log_level=int(config["LOG_LEVEL"]),
                           local_cache=config["STATIC_STORAGE_CACHE_DIR"])
    main = Main(config=config, file_storage=fs, static_storage=ss)
    main.start(True)
