import logging
import os
import signal

from DicomFlowLib.conf import load_configs
from DicomFlowLib.log import init_logger
from DicomFlowLib.mq import MQSub
from DicomFlowLib.mq import PubModel, SubModel
from scheduler.impl import Scheduler


class Main:
    def __init__(self, config):
        signal.signal(signal.SIGTERM, self.stop)

        self.running = None
        init_logger(name=config["LOG_NAME"],
                    log_format=config["LOG_FORMAT"],
                    log_dir=config["LOG_DIR"],
                    rabbit_hostname=config["RABBIT_HOSTNAME"],
                    rabbit_port=int(config["RABBIT_PORT"]),
                    pub_models=[PubModel(**d) for d in config["LOG_PUB_MODELS"]])

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(int(config["LOG_LEVEL"]))

        self.scheduler = Scheduler(pub_routing_key_success=config["PUB_ROUTING_KEY_SUCCESS"],
                                   pub_routing_key_fail=config["PUB_ROUTING_KEY_FAIL"],
                                   pub_routing_key_gpu=config["PUB_ROUTING_KEY_GPU"],
                                   pub_routing_key_cpu=config["PUB_ROUTING_KEY_CPU"],
                                   log_level=int(config["LOG_LEVEL"]),
                                   consumer_exchange=config["SUB_CONSUMER_EXCHANGE"],
                                   reschedule_priority=config["SUB_RESCHEDULE_PRIORITY"],
                                   database_path=config["DATABASE_PATH"])

        self.mq = MQSub(rabbit_hostname=config["RABBIT_HOSTNAME"], rabbit_port=int(config["RABBIT_PORT"]),
                        sub_models=[SubModel(**d) for d in config["SUB_MODELS"]],
                        pub_models=[PubModel(**d) for d in config["PUB_MODELS"]],
                        work_function=self.scheduler.mq_entrypoint,
                        sub_prefetch_value=int(config["SUB_PREFETCH_COUNT"]),
                        sub_queue_kwargs=config["SUB_QUEUE_KWARGS"],
                        pub_routing_key_error=config["PUB_ROUTING_KEY_ERROR"],
                        log_level=int(config["LOG_LEVEL"]))

    def start(self):
        self.running = True

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

    def stop(self, signalnum=None, stack_frame=None):
        self.running = False
        self.mq.stop()
        self.mq.join()


if __name__ == "__main__":
    config = load_configs(os.environ["CONF_DIR"], os.environ["CURRENT_CONF"])

    m = Main(config=config)
    m.start()
