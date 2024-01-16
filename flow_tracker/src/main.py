import os
import signal

import yaml
from pydantic.v1.utils import deep_update

from DicomFlowLib.conf import load_configs
from DicomFlowLib.log import CollectiveLogger
from flow_tracker import FlowTracker


class Main:
    def __init__(self, config):
        signal.signal(signal.SIGTERM, self.stop)
        self.running = False
        self.logger = CollectiveLogger(name=config["LOG_NAME"],
                                       log_level=int(config["LOG_LEVEL"]),
                                       log_format=config["LOG_FORMAT"],
                                       log_dir=config["LOG_DIR"],
                                       rabbit_hostname=config["RABBIT_HOSTNAME"],
                                       rabbit_port=int(config["RABBIT_PORT"]),
                                       rabbit_password=config["RABBIT_PASSWORD"],
                                       rabbit_username=config["RABBIT_USERNAME"])

        self.ft = FlowTracker(logger=self.logger,
                              sub_exchanges=config["SUB_EXCHANGES"],
                              sub_prefetch_value=int(config["SUB_PREFETCH_COUNT"]),
                              rabbit_hostname=config["RABBIT_HOSTNAME"],
                              rabbit_port=int(config["RABBIT_PORT"]))

    def start(self):
        self.logger.debug("Starting FlowTracker", finished=False)
        self.running = True
        self.logger.start()
        self.ft.start()
        self.logger.debug("Starting FlowTracker", finished=True)

        while self.running:
            try:
                self.ft.join(timeout=5)
                if self.ft.is_alive():
                    pass
                else:
                    self.stop()
            except KeyboardInterrupt:
                self.stop()

    def stop(self):
        self.logger.debug("Stopping FlowTracker", finished=False)
        self.running = False
        self.ft.stop()
        self.logger.stop()

        self.ft.join()
        self.logger.join()
        self.logger.debug("Stopping SCU", finished=True)


if __name__ == "__main__":
    config = load_configs(os.environ["CONF_DIR"])

    m = Main(config=config)
    m.start()
