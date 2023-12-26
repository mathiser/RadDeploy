import queue

from python_logging_rabbitmq import RabbitMQHandler

from DicomFlowLib.default_config import Config
from DicomFlowLib.fs.file_storage import FileStorage
from DicomFlowLib.mq import MQPub
from scp import SCP


def main():
    config = Config["storescp"]
    fs = FileStorage(**Config["fs_base"])
    publish_queue = queue.Queue()
    logger = RabbitMQHandler(**Config["logger_base"])
    SCP(mq_logger=logger, file_storage=fs, publish_queue=publish_queue, **config["scp"]).run_scp(blocking=False)

    mq = MQPub(logger=logger, **Config["mq_base"], publish_queue=publish_queue, **config["mq_pub"])
    mq.start()
    mq.join()





if __name__ == "__main__":
    main()
