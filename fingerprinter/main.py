import queue
import traceback

import yaml

from DicomFlowLib.default_config import Config
from DicomFlowLib.mq import MQSub, MQPub
from fingerprinting import Fingerprinter


def main():
    config = Config["fingerprinting"]["mq_sub"]
    sc = queue.Queue()
    fp = Fingerprinter(scheduled_contexts=sc, **Config["fingerprinting"]["fingerprinter"])
    MQSub(**Config["mq_base"], **Config["fingerprinting"]["mq_sub"], work_function=fp.run_fingerprinting).run()  # blocks


if __name__ == "__main__":
    with open("../fingerprints/test.yaml") as r:
        y = yaml.safe_load(r)
    print(y)
    main()
