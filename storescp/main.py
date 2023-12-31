import queue

from DicomFlowLib.default_config import Config
from DicomFlowLib.fs.file_storage import FileStorage
from DicomFlowLib.log.logger import CollectiveLogger
from DicomFlowLib.mq import MQPub
from scp import SCP


class Main:
    def __init__(self, config=Config):
        self.running = True
        self.publish_queue = queue.Queue()
        self.logger = CollectiveLogger(**config["storescp"]["logger"])
        self.fs = FileStorage(logger=self.logger, **config["fs_base"])
        self.scp = SCP(logger=self.logger, file_storage=self.fs, publish_queue=self.publish_queue, **config["storescp"]["scp"])
        self.mq = MQPub(logger=self.logger, **config["mq_base"], publish_queue=self.publish_queue, **config["storescp"]["mq_pub"])

    def start(self):
        self.logger.debug("Starting")
        self.scp.start(blocking=False)
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
        self.scp.stop()
        self.logger.stop()

        self.mq.join()
        self.logger.join()



if __name__ == "__main__":
    m = Main()
    m.start()
