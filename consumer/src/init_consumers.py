import logging
import os
from multiprocessing import Process

from DicomFlowLib.conf import load_configs
from DicomFlowLib.log import init_logger
from DicomFlowLib.mq import PubModel
from docker_consumer import Worker
from main import MainConsumer


def run_consumer(config, worker):
    m = MainConsumer(config=config, worker=worker)
    m.start()


def main():
    config = load_configs(os.environ["CONF_DIR"], os.environ["CURRENT_CONF"])
    init_logger(name=config["LOG_NAME"],
                log_format=config["LOG_FORMAT"],
                log_dir=config["LOG_DIR"],
                rabbit_hostname=config["RABBIT_HOSTNAME"],
                rabbit_port=int(config["RABBIT_PORT"]),
                pub_models=[PubModel(**d) for d in config["LOG_PUB_MODELS"]])
    logger = logging.getLogger(__name__)
    logger.setLevel(int(config["LOG_LEVEL"]))
    workers = []
    assert isinstance(config["CPUS"], int)
    for cpu_id in range(config["CPUS"]):
        logger.info(f"Spawning CPU worker: {cpu_id}")
        p = Process(target=MainConsumer, args=(config, Worker(type="CPU", device_id=str(cpu_id)),))
        workers.append(p)
        p.start()

    if isinstance(config["GPUS"], str):
        device_ids = config["GPUS"].split()
    else:
        device_ids = config["GPUS"]

    for device_id in device_ids:
        logger.info(f"Spawning GPU worker: {device_id}")
        p = Process(target=MainConsumer, args=(config, Worker(type="CPU", device_id=str(device_id)),))
        workers.append(p)
        p.start()

    for p in workers:
        p.join()


if __name__ == "__main__":
    main()
