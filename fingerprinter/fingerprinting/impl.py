import json
import logging
import os
import tarfile
from copy import copy
from io import BytesIO
from multiprocessing.pool import ThreadPool
from typing import Dict, List

import pandas as pd
import pydicom
import yaml
from python_logging_rabbitmq import RabbitMQHandler

from DicomFlowLib.data_structures.contexts import FlowContext
from DicomFlowLib.data_structures.flow import Flow
from DicomFlowLib.default_config import LOG_FORMAT
from DicomFlowLib.mq import MQBase


class Fingerprinter:
    def __init__(self,
                 pub_routing_key: str,
                 pub_routing_key_as_queue: bool,
                 flow_directory: str,
                 log_level: int,
                 mq_logger: RabbitMQHandler | None = None,
                 pub_exchange: str = "",
                 pub_exchange_type: str = "direct"):
        self.mq_logger = mq_logger
        self.pub_routing_key_as_queue = pub_routing_key_as_queue
        self.pub_exchange = pub_exchange
        self.pub_exchange_type = pub_exchange_type
        self.pub_routing_key = pub_routing_key
        self.flows = []
        self.flow_directory = flow_directory
        logging.basicConfig(level=log_level, format=LOG_FORMAT)
        self.LOGGER = logging.getLogger(__name__)
        if mq_logger:
            self.LOGGER.addHandler(mq_logger)

        self.reload_fingerprints()

    def parse_file_metas(self, file_metas: List) -> pd.DataFrame:
        def parse_meta(file_meta):
            elems = {}
            for elem in pydicom.Dataset.from_json(file_meta):
                elems[str(elem.keyword)] = str(elem.value)
            return elems

        t = ThreadPool(16)
        res = t.map(parse_meta, file_metas)
        t.close()
        t.join()

        return pd.DataFrame(res)

    def fingerprint(self, ds: pd.DataFrame, triggers: List):
        match = ds
        for trigger in triggers:
            for keyword, regex_pattern in trigger.items():
                match = match[match[keyword].str.contains(regex_pattern, regex=True)]  # Regex match. This is "recursive"
                                                                                       # matching.
        return bool(len(match))

    def mq_entrypoint(self, connection, channel, basic_deliver, properties, body):
        self.reload_fingerprints()

        context = FlowContext(**json.loads(body.decode()))
        mq = MQBase(self.mq_logger)
        mq.connect_with(connection, channel)

        mq.setup_exchange_callback(self.pub_exchange)
        mq.setup_queue_and_bind_callback(exchange=self.pub_exchange,
                                         routing_key=self.pub_routing_key,
                                         routing_key_as_queue=self.pub_routing_key_as_queue)

        ds = self.parse_file_metas(context.file_metas)
        for flow in self.flows:
            if self.fingerprint(ds, flow.triggers):
                context.flow = flow.copy()
                mq.basic_publish_callback(exchange=self.pub_exchange,
                                          routing_key=self.pub_routing_key,
                                          body=context.model_dump_json().encode())

    def reload_fingerprints(self):
        self.flows = []
        for fol, subs, files in os.walk(self.flow_directory):
            for file in files:
                fp_path = os.path.join(fol, file)
                try:
                    with open(fp_path) as r:
                        fp = yaml.safe_load(r)
                        flow = Flow(**fp)
                        self.flows.append(flow)

                except Exception as e:
                    self.LOGGER.error(str(e))
                    raise e
        self.LOGGER.debug("Loaded the following flow definitions:")
        self.LOGGER.debug(self.flows)
