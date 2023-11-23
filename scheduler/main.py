import yaml

from DicomFlowLib.default_config import Config
from DicomFlowLib.mq import MQSub, MQPub
from scheduler.fingerprinter import Fingerprinter


def main():
    mq_pub = MQPub(**Config["mq_base"], **Config["scheduler"]["mq_pub"])
    fp = Fingerprinter(mq_pub=mq_pub, **Config["scheduler"]["fingerprinter"])
    mq = MQSub(**Config["mq_base"], **Config["scheduler"]["mq_sub"], work_function=fp.fingerprint)
    mq.subscribe()

if __name__ == "__main__":
    with open("./fingerprints/test.yaml") as r:
        y = yaml.safe_load(r)
    print(y)
    main()