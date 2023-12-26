import yaml
from python_logging_rabbitmq import RabbitMQHandler

from DicomFlowLib.default_config import Config
from DicomFlowLib.mq import MQSub
from fingerprinting import Fingerprinter


def main():
    logger = RabbitMQHandler(**Config["logger_base"])
    fp = Fingerprinter(mq_logger=logger, **Config["fingerprinter"]["fingerprinting"])

    mq = MQSub(mq_logger=logger,
               **Config["mq_base"],
               **Config["fingerprinter"]["mq_sub"],
               work_function=fp.mq_entrypoint)
    mq.start()
    mq.join()  # blocks


if __name__ == "__main__":
    main()
