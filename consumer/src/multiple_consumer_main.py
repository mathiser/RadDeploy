import logging
import os
from multiprocessing import Process
from typing import Dict

from DicomFlowLib.conf import load_configs
from DicomFlowLib.fs import FileStorageClient
from DicomFlowLib.mq import PubModel, SubModel
from docker_executor import Worker
from single_consumer_main import MainConsumer


def run_consumer(config: Dict, worker: Worker, sub_models: Dict, sub_queue_kwargs: Dict):
    file_storage = FileStorageClient(file_storage_url=config["FILE_STORAGE_URL"],
                                     log_level=config["LOG_LEVEL"])

    static_storage = FileStorageClient(file_storage_url=config["STATIC_STORAGE_URL"],
                                       log_level=config["LOG_LEVEL"],
                                       local_cache="/opt/DicomFlow/static/")

    m = MainConsumer(log_level=config["LOG_LEVEL"],
                     worker=worker,
                     rabbit_port=config["RABBIT_PORT"],
                     rabbit_hostname=config["RABBIT_HOSTNAME"],
                     file_storage=file_storage,
                     static_storage=static_storage,
                     log_dir=config["LOG_DIR"],
                     log_format=config["LOG_FORMAT"],
                     pub_models=[PubModel(**pm) for pm in config["PUB_MODELS"]],
                     sub_models=[SubModel(**sm) for sm in sub_models],
                     log_pub_models=[PubModel(**pm) for pm in config["LOG_PUB_MODELS"]],
                     sub_queue_kwargs=sub_queue_kwargs,
                     sub_prefetch_value=config["SUB_PREFETCH_COUNT"])
    m.start(blocking=True)
    return m


def main(config):
    logger = logging.getLogger(__file__)
    workers = []
    cpus = int(config["CPUS"])
    for cpu_id in range(cpus):
        logger.info(f"Spawning CPU worker: {cpu_id}")
        p = Process(target=run_consumer, args=(config,
                                               Worker(type="CPU", device_id=str(cpu_id)),
                                               config["CPU_SUB_MODELS"],
                                               config["CPU_SUB_QUEUE_KWARGS"])).start()
        workers.append(p)

    if isinstance(config["GPUS"], str):
        device_ids = config["GPUS"].split()
    else:
        device_ids = config["GPUS"]

    for device_id in device_ids:
        logger.info(f"Spawning GPU worker: {device_id}")
        p = Process(target=run_consumer, args=(config,
                                               Worker(type="GPU", device_id=str(device_id)),
                                               config["GPU_SUB_MODELS"],
                                               config["GPU_SUB_QUEUE_KWARGS"]))
        workers.append(p)
        p.start()

    for p in workers:
        p.join()


if __name__ == "__main__":
    config = load_configs(os.environ["CONF_DIR"], os.environ["CURRENT_CONF"])
    main(config=config)
