import os

import dotenv

from scheduler.mq.sub import MQSub

dotenv.load_dotenv(".env")

def main():
    mq = MQSub(hostname=os.environ.get("MQ_HOSTNAME"),
               port=int(os.environ.get("MQ_PORT")),
               log_level=int(os.environ.get("LOG_LEVEL")),
               queue_name=os.environ.get("STORESCP_QUEUE"),
               prefetch_value=1)
    mq.subscribe()

if __name__ == "__main__":
    main()