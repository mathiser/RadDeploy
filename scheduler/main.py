import yaml

from DicomFlowLib.default_config import Config
from DicomFlowLib.mq import MQSub, MQPub
from scheduler.fingerprinter import Fingerprinter


def main():
    mq_pub = MQPub(**Config["mq_base"], **Config["scheduler"]["mq_pub"])
    fp = Fingerprinter(mq=mq_pub, **Config["scheduler"]["fingerprinter"])
    mq_sub = MQSub(**Config["mq_base"], **Config["scheduler"]["mq_sub"], work_function=fp.run_fingerprinting)
    mq_sub.subscribe()

if __name__ == "__main__":
    with open("./fingerprints/test.yaml") as r:
        y = yaml.safe_load(r)
    print(y)
    main()