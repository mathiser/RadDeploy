import yaml
from python_logging_rabbitmq import RabbitMQHandler

from DicomFlowLib.default_config import Config
from DicomFlowLib.log.logger import CollectiveLogger
from DicomFlowLib.mq import MQSub
from fingerprinting import Fingerprinter


class Main:
    def __init__(self, config=Config):
        self.running = True

        self.logger = CollectiveLogger(**Config["fingerprinter"]["logger"])
        self.fp = Fingerprinter(logger=self.logger,
                                **Config["fingerprinter"]["fingerprinting"])
        self.mq = MQSub(logger=self.logger,
                        **Config["mq_base"],
                        **Config["fingerprinter"]["mq_sub"],
                        work_function=self.fp.mq_entrypoint)

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


if __name__ == "__main__":
    m = Main()
    m.start()
