import traceback

import yaml

from DicomFlowLib.default_config import Config
from DicomFlowLib.mq import MQSub
from fingerprinting import Fingerprinter


def main():
    config = Config["fingerprinting"]["mq_sub"]
    fp = Fingerprinter(**Config["fingerprinting"]["fingerprinter"])
    mq = MQSub(**Config["mq_base"], **Config["fingerprinting"]["mq_sub"], work_function=fp.run_fingerprinting)

    try:
        mq.run()
    except Exception as e:
        print(traceback.format_exc())
        print(str(e))
        raise e
    finally:
        pass

if __name__ == "__main__":
    with open("../fingerprints/test.yaml") as r:
        y = yaml.safe_load(r)
    print(y)
    main()
