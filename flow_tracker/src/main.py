import logging
import os
import signal

from DicomFlowLib.conf import load_configs
from DicomFlowLib.log import init_logger
from DicomFlowLib.mq import SubModel, PubModel
from DicomFlowLib.mq import MQSub
from flow_tracker import FlowTracker
import time


class Main:
    def __init__(self, config):
        signal.signal(signal.SIGTERM, self.stop)
        self.running = False
        init_logger(name=config["LOG_NAME"],
                                         log_format=config["LOG_FORMAT"],
                                         log_dir=config["LOG_DIR"],
                                         rabbit_hostname=config["RABBIT_HOSTNAME"],
                                         rabbit_port=int(config["RABBIT_PORT"]),
                                         pub_models=[PubModel(**d) for d in config["LOG_PUB_MODELS"]], )
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(int(config["LOG_LEVEL"]))

        self.ft = FlowTracker(database_path=config["DATABASE_PATH"],
                              dashboard_rules=config["DASHBOARD_RULES"])

        self.mq = MQSub(work_function=self.ft.mq_entrypoint,
                        rabbit_hostname=config["RABBIT_HOSTNAME"],
                        rabbit_port=int(config["RABBIT_PORT"]),
                        sub_models=[SubModel(**d) for d in config["SUB_MODELS"]],
                        pub_models=[],
                        sub_prefetch_value=int(config["SUB_PREFETCH_COUNT"]),
                        sub_queue_kwargs=config["SUB_QUEUE_KWARGS"],
                        pub_routing_key_error=config["PUB_ROUTING_KEY_ERROR"])

    def start(self):
        self.logger.debug("Starting FlowTracker")
        self.running = True

        self.mq.start()
        self.logger.debug("Starting FlowTracker")

        while self.running:
            try:
                self.mq.join(timeout=5)
                if self.mq.is_alive():
                    pass
                else:
                    self.stop()
            except KeyboardInterrupt:
                self.stop()

    def stop(self, signalnum=None, stack_frame=None):
        self.logger.debug("Stopping FlowTracker")
        self.running = False
        self.mq.stop()

        self.mq.join()
        self.logger.debug("Stopping FlowTracker")


if __name__ == "__main__":
    config = load_configs(os.environ["CONF_DIR"], os.environ["CURRENT_CONF"])

    m = Main(config=config)
    m.start()
