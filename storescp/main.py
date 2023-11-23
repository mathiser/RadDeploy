from scp import SCP
from DicomFlowLib.mq import MQPub
from DicomFlowLib.default_config import Config


def main():
    mq = MQPub(**Config["mq_base"], **Config["storescp"]["mq_pub"])
    SCP(mq=mq, **Config["storescp"]["scp"]).run_scp(blocking=True)


if __name__ == "__main__":
    main()
