import queue

from DicomFlowLib.default_config import Config
from DicomFlowLib.mq import MQPub
from scp import SCP


def main():
    config = Config["storescp"]

    q = queue.Queue()
    SCP(publish_queue=q, **config["scp"]).run_scp(blocking=False)

    MQPub(**Config["mq_base"], publish_queue=q, **config["mq_pub"]).start()





if __name__ == "__main__":
    main()
