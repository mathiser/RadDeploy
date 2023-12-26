from python_logging_rabbitmq import RabbitMQHandler

from DicomFlowLib.default_config import Config
from DicomFlowLib.fs import FileStorage
from DicomFlowLib.mq import MQSub

from scu import SCU


def main():
    logger = RabbitMQHandler(**Config["logger_base"])
    fs = FileStorage(**Config["fs_base"])
    scu = SCU(file_storage=fs, mq_logger=logger, **Config["storescu"]["storescu"])
    mq = MQSub(mq_logger=logger, **Config["mq_base"],
          **Config["storescu"]["mq_sub"],
          work_function=scu.mq_entrypoint)
    mq.start()
    mq.join()


if __name__ == "__main__":
    main()
