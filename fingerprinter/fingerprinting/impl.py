import logging
import os
import re
import traceback
from typing import Dict, Set, List

import pika.channel
import pydicom
import yaml

from DicomFlowLib.contexts import Destination, Flow, Node
from DicomFlowLib.default_config import LOG_FORMAT


class Fingerprinter:
    def __init__(self,
                 pub_exchange: str,
                 pub_queue: str,
                 flow_directory: str,
                 log_level: int):
        self.pub_exchange = pub_exchange
        self.pub_queue = pub_queue
        self.flows = []
        self.flow_directory = flow_directory

        logging.basicConfig(level=log_level, format=LOG_FORMAT)
        self.logger = logging.getLogger(__name__)

    def parse_file_metas(self, context: Dict) -> Dict[str, Set]:
        ds = {}
        for file_meta in context["file_metas"]:
            for elem in pydicom.Dataset.from_json(file_meta):
                if elem.keyword not in ds.keys():
                    ds[str(elem.keyword)] = {str(elem.value)}
                else:
                    ds[elem.keyword].add(str(elem.value))
        return ds

    def fingerprint(self, ds: Dict[str, Set], triggers: List):
        for trigger in triggers:
            print(trigger)
            for keyword, regex_pattern in trigger.items():
                pattern = re.compile(regex_pattern)

                for val in ds[keyword]:
                    if pattern.search(val):
                        break
                else:
                    return False
        else:
            return True

    def run_fingerprinting(self, channel: pika.channel.Channel, context: Dict):

        print("!")
        print(context)
        self.reload_fingerprints()
        print(self.flows)

        #ds = self.parse_file_metas(context)

        #for flow in self.flows:
            # if self.fingerprint(ds, flow.triggers):
            #
            #     scheduler_context = SchedulerContext(
            #         scp_context=context,
            #         fingerprint=flow.model_dump())
            #
            #     self.logger.info(f"Scheduling flows on fingerprint {flow}")
            #     self.mq.publish(context=scheduler_context,
            #                     exchange=self.pub_exchange,
            #                     queue_or_routing_key=self.pub_queue)

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

