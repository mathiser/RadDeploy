import json
import os
from multiprocessing.pool import ThreadPool
from typing import List

import pandas as pd
import pydicom
import yaml

from DicomFlowLib.data_structures.contexts import FlowContext
from DicomFlowLib.data_structures.flow import Flow
from DicomFlowLib.log.logger import CollectiveLogger
from DicomFlowLib.mq import MQBase


class Fingerprinter:
    def __init__(self,
                 pub_routing_key: str,
                 pub_routing_key_as_queue: bool,
                 logger: CollectiveLogger,
                 flow_directory: str,
                 pub_exchange: str,
                 pub_exchange_type: str):
        self.logger = logger

        self.pub_routing_key_as_queue = pub_routing_key_as_queue
        self.pub_exchange = pub_exchange
        self.pub_exchange_type = pub_exchange_type
        self.pub_routing_key = pub_routing_key
        self.flows = []
        self.flow_directory = flow_directory

        self.reload_fingerprints()
        self.uid = None

    def parse_file_metas(self, file_metas: List) -> pd.DataFrame:
        self.logger.info(f"PARSING FILE METAS", uid=self.uid, finished=False)

        def parse_meta(file_meta):
            elems = {}
            for elem in pydicom.Dataset.from_json(file_meta):
                elems[str(elem.keyword)] = str(elem.value)
            return elems

        t = ThreadPool(16)
        res = t.map(parse_meta, file_metas)
        t.close()
        t.join()
        self.logger.info(f"PARSING FILE METAS", uid=self.uid, finished=True)
        return pd.DataFrame(res)

    def fingerprint(self, ds: pd.DataFrame, triggers: List):
        self.logger.info(f"RUNNING FINGERPRINTING", uid=self.uid, finished=False)
        match = ds
        for trigger in triggers:
            for keyword, regex_pattern in trigger.items():
                match = match[
                    match[keyword].str.contains(regex_pattern, regex=True)]  # Regex match. This is "recursive"
                # matching.
        is_match = bool(len(match))
        if is_match:
            self.logger.info(f"FOUND MATCH", uid=self.uid, finished=True)

        self.logger.info(f"RUNNING FINGERPRINTING", uid=self.uid, finished=True)
        return bool(len(match))

    def mq_entrypoint(self, connection, channel, basic_deliver, properties, body):
        self.reload_fingerprints()

        context = FlowContext(**json.loads(body.decode()))
        self.uid = context.uid
        mq = MQBase(self.logger).connect_with(connection, channel)

        mq.setup_exchange_callback(self.pub_exchange)
        mq.setup_queue_and_bind_callback(exchange=self.pub_exchange,
                                         routing_key=self.pub_routing_key,
                                         routing_key_as_queue=self.pub_routing_key_as_queue)
        ds = self.parse_file_metas(context.file_metas)
        for flow in self.flows:
            if self.fingerprint(ds, flow.triggers):
                context.flow = flow.copy()
                self.logger.info(f"PUB TO QUEUE: {self.pub_routing_key}", uid=self.uid, finished=False)
                mq.basic_publish_callback(exchange=self.pub_exchange,
                                          routing_key=self.pub_routing_key,
                                          body=context.model_dump_json().encode(),
                                          priority=context.flow.priority)
                self.logger.info(f"PUB TO QUEUE: {self.pub_routing_key}", uid=self.uid, finished=True)

        self.uid = None
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
                    self.logger.error(str(e))
                    raise e
        self.logger.debug("Loaded the following flow definitions:")
        self.logger.debug(self.flows)
