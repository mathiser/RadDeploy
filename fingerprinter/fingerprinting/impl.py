import json
import logging
import os
from multiprocessing.pool import ThreadPool
from typing import Dict, List

import pandas as pd
import pydicom
import yaml

from DicomFlowLib.data_structures import contexts
from DicomFlowLib.default_config import LOG_FORMAT
from DicomFlowLib.mq.base import MQBase


class Fingerprinter:
    def __init__(self,
                 pub_routing_key: str,
                 pub_routing_key_as_queue: bool,
                 flow_directory: str,
                 log_level: int,
                 pub_exchange: str = "",
                 pub_exchange_type: str = "direct"):

        self.pub_routing_key_as_queue = pub_routing_key_as_queue
        self.pub_exchange = pub_exchange
        self.pub_exchange_type = pub_exchange_type
        self.pub_routing_key = pub_routing_key
        self.flows = []
        self.flow_directory = flow_directory
        logging.basicConfig(level=log_level, format=LOG_FORMAT)
        self.LOGGER = logging.getLogger(__name__)

        self.reload_fingerprints()

        self.pub_exchange_and_queue_declared = False
    def parse_file_metas(self, context: Dict) -> pd.DataFrame:
        def parse_meta(file_meta):
            elems = {}
            for elem in pydicom.Dataset.from_json(file_meta):
                elems[str(elem.keyword)] = str(elem.value)
            return elems

        t = ThreadPool(16)
        res = t.map(parse_meta, context["file_metas"])
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

    def run_fingerprinting(self, connection, channel, basic_deliver, properties, body):
        self.reload_fingerprints()
        context = json.loads(body)

        mq = MQBase()
        mq.connect(connection, channel)

        if not self.pub_exchange_and_queue_declared:
            mq.setup_exchange_callback(self.pub_exchange)
            mq.setup_queue_callback(exchange=self.pub_exchange,
                                    routing_key=self.pub_routing_key,
                                    routing_key_as_queue=self.pub_routing_key_as_queue)

        ds = self.parse_file_metas(context)
        for flow in self.flows:
            if self.fingerprint(ds, flow.triggers):
                flow_context = flow.copy()
                flow_context.file_exchange = context["file_exchange"]
                flow_context.file_queue = context["file_queue"]

                mq.basic_publish_callback(exchange=self.pub_exchange,
                                          routing_key=self.pub_routing_key,
                                          body=flow_context.model_dump_json().encode())
                self.LOGGER.info(flow_context.model_dump_json())
    def reload_fingerprints(self):
        self.flows = []
        for fol, subs, files in os.walk(self.flow_directory):
            for file in files:
                fp_path = os.path.join(fol, file)
                try:
                    with open(fp_path) as r:
                        fp = yaml.safe_load(r)
                    # Validate that correct things are in the fp yaml
                    if self.validate_fingerprint(fp):
                        destinations = [contexts.Destination(**d) for d in fp["destinations"]]
                        model = contexts.Model(**fp["model"])
                        flow = contexts.FlowContext(
                            destinations=destinations,
                            model=model,
                            triggers=fp["triggers"])

                        self.flows.append(flow)
                except Exception as e:
                    self.LOGGER.error(str(e))
                    raise e
        self.LOGGER.debug("Loaded the following flow definitions:")
        self.LOGGER.debug(self.flows)

    def validate_fingerprint(self, fingerprint: Dict):
        assert "triggers" in fingerprint.keys()
        assert isinstance(fingerprint["triggers"], list)
        assert len(fingerprint["triggers"]) != 0

        assert "model" in fingerprint.keys()
        assert isinstance(fingerprint["model"], dict)

        assert "destinations" in fingerprint.keys()
        assert isinstance(fingerprint["destinations"], List)

        for dest in fingerprint["destinations"]:
            assert "hostname" in dest.keys()
            assert "port" in dest.keys()
            assert "ae_title" in dest.keys()

        return True

