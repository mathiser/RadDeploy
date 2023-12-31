from python_logging_rabbitmq import RabbitMQHandler

from DicomFlowLib.default_config import Config
from DicomFlowLib.fs import FileStorage
from DicomFlowLib.log.logger import CollectiveLogger
from DicomFlowLib.mq import MQSub

from docker_consumer.impl import DockerConsumer


class Main:
    def __init__(self, config=Config):
        self.running=True
        self.logger = CollectiveLogger(**config["consumer"]["logger"])

        self.fs = FileStorage(logger=self.logger, **config["fs_base"])
        self.consumer = DockerConsumer(logger=self.logger, file_storage=self.fs, **config["consumer"]["consumer"])

        self.mq = MQSub(logger=self.logger,
                        **config["mq_base"],
                        **config["consumer"]["mq_sub"],
                        work_function=self.consumer.mq_entrypoint)

    def start(self):
        self.logger.start()
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

    def stop(self):
        self.running = False
        self.mq.stop()
        self.logger.stop()

        self.mq.join()
        self.logger.join()


if __name__ == "__main__":
    m = Main()
    m.start()
