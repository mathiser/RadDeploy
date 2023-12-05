import yaml

from DicomFlowLib.default_config import Config
from DicomFlowLib.mq import MQPubSub, MQPub
from fingerprinter.fingerprinting import Fingerprinter


def main():
    mq_pub = MQPub(**Config["mq_base"], **Config["consumer"]["mq_pub"])
    fp = DockerConsumer()
    mq_sub = MQPubSub(**Config["mq_base"], **Config["fingerprinting"]["mq_sub"], work_function=fp.run_fingerprinting)
    mq_sub.subscribe()

if __name__ == "__main__":
    main()