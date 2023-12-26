from python_logging_rabbitmq import RabbitMQHandler

from DicomFlowLib.default_config import Config
from DicomFlowLib.fs import FileStorage
from DicomFlowLib.mq import MQSub

from docker_consumer.impl import DockerConsumer


def main():
    logger = RabbitMQHandler(**Config["logger_base"])

    fs = FileStorage(**Config["fs_base"])
    consumer = DockerConsumer(logger=logger, file_storage=fs, **Config["consumer"]["consumer"])

    mq = MQSub(mq_logger=logger,
               **Config["mq_base"],
               **Config["consumer"]["mq_sub"],
               work_function=consumer.mq_entrypoint)
    mq.start()
    mq.join()


if __name__ == "__main__":
    main()
