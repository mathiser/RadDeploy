import queue

from DicomFlowLib.default_config import Config
from DicomFlowLib.mq.pub import MQPub
from scp import SCP


def main():
    cs = queue.Queue()
    print("here")
    SCP(**Config["storescp"]["scp"], scheduled_contexts=cs).run_scp(blocking=False)
    print("after")
    MQPub(**Config["mq_base"], pub_interval=1, scheduled_contexts=cs).run()





if __name__ == "__main__":
    main()
