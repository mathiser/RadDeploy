import json
import logging
import os
import re
import time
import traceback
from queue import Queue
from typing import Dict, Set, List

import pika.channel
import pydicom
import yaml
import pandas as pd
from DicomFlowLib.contexts import Destination, Flow, Node, SchedulerContext
from DicomFlowLib.default_config import LOG_FORMAT


class Fingerprinter:
    def __init__(self,
                 scheduled_contexts: Queue,
                 pub_exchange: str,
                 pub_queue: str,
                 flow_directory: str,
                 log_level: int):
        self.pub_exchange = pub_exchange
        self.pub_queue = pub_queue
        self.flows = []
        self.flow_directory = flow_directory
        self.scheduled_contexts = scheduled_contexts
        logging.basicConfig(level=log_level, format=LOG_FORMAT)
        self.logger = logging.getLogger(__name__)

    def parse_file_metas(self, context: Dict) -> pd.DataFrame:
        l = []
        print(context)
        time.sleep(30)
        for file_meta in context["file_metas"]:
            try:
                elems = {}
                for elem in pydicom.Dataset.from_json(file_meta):
                    elems[str(elem.keyword)] = str(elem.value)
                l.append(elems)
            except Exception as e:
                print(e)

        return pd.DataFrame(l)

    def fingerprint(self, ds: pd.DataFrame, triggers: List):
        match = ds
        for trigger in triggers:
            for keyword, regex_pattern in trigger.items():
                match = match[match[keyword].str.contains(regex_pattern, regex=True)]  # Regex match. This is "recursive"
                                                                                       # matching.

        return bool(len(match))

    def run_fingerprinting(self, channel: pika.channel.Channel, context: Dict):
        self.reload_fingerprints()
        print(self.pub_exchange, self.pub_queue)
        ds = self.parse_file_metas(context)

        for flow in self.flows:
            if self.fingerprint(ds, flow.triggers):

                pub_context = flow.model_dump()
                pub_context["file_exchange"] = context["file_exchange"]
                pub_context["file_routing_key"] = context["file_routing_key"]
                channel.exchange_declare(self.pub_exchange)
                channel.queue_declare(self.pub_queue)
                channel.queue_bind(queue=self.pub_queue, exchange=self.pub_exchange)
                channel.basic_publish(exchange=self.pub_exchange,
                                      routing_key=self.pub_queue,
                                      body=json.dumps(pub_context).encode())

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
                        print(fp)
                        destinations = [Destination(**d) for d in fp["destinations"]]
                        print(destinations)
                        nodes = [Node(**n) for n in fp["nodes"]]
                        print(nodes)

                        flow = Flow(
                            destinations=destinations,
                            nodes=nodes,
                            triggers=fp["triggers"])
                        print(flow)
                        self.flows.append(flow)
                except Exception as e:
                    self.logger.error(str(e))
                    print(traceback.format_exc())
                    raise e
    def validate_fingerprint(self, fingerprint: Dict):
        assert "triggers" in fingerprint.keys()
        assert isinstance(fingerprint["triggers"], list)
        assert len(fingerprint["triggers"]) != 0

        assert "nodes" in fingerprint.keys()
        assert isinstance(fingerprint["nodes"], list)

        assert "destinations" in fingerprint.keys()
        assert isinstance(fingerprint["destinations"], list)

        for dest in fingerprint["destinations"]:
            assert "hostname" in dest.keys()
            assert "port" in dest.keys()
            assert "ae_title" in dest.keys()

        return True

