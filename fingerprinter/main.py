import queue

import yaml

from DicomFlowLib.default_config import Config
from DicomFlowLib.mq import MQSub
from fingerprinting import Fingerprinter


def main():
    fp = Fingerprinter(**Config["fingerprinter"]["fingerprinting"])
    MQSub(**Config["mq_base"],
          **Config["fingerprinter"]["mq_sub"],
          work_function=fp.run_fingerprinting).consume()  # blocks


if __name__ == "__main__":
    with open("flow_definitions/test.yaml") as r:
        y = yaml.safe_load(r)
    print(y)
    main()
