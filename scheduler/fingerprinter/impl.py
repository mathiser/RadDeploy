import logging
import os
from typing import Dict, Set
import re
import pydicom
import yaml
from DicomFlowLib.contexts import SCPContext, SchedulerContext
from DicomFlowLib.mq import MQPub

LOG_FORMAT = '%(levelname)s:%(asctime)s:%(message)s'

class Fingerprinter:
    def __init__(self,
                 mq: MQPub,
                 pub_queue: str,
                 fingerprint_directory: str,
                 log_level: int):
        self.mq = mq
        self.pub_queue = pub_queue
        self.fingerprints = []
        self.fingerprints_directory = fingerprint_directory

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

    def fingerprint(self, ds: Dict[str, Set], fp: Dict):
        for trigger in fp["triggers"]:
            for keyword, regex_pattern in trigger.items():
                pattern = re.compile(regex_pattern)

                for val in ds[keyword]:
                    if pattern.search(val):
                        break
                else:
                    return False
        else:
            return True

    def run_fingerprinting(self, context: Dict):
        self.reload_fingerprints()
        ds = self.parse_file_metas(context)

        for fp in self.fingerprints:
            if self.fingerprint(ds, fp):
                self.logger.info(f"Matched {ds} to {fp}")
                scheduler_context = SchedulerContext(
                    exchange=self.mq.exchange,
                    queue=self.pub_queue,
                    scp_context=context,
                    fingerprint=fp)
                print(scheduler_context)
                self.mq.publish(context=scheduler_context,
                                queue_or_routing_key=self.pub_queue)


    def reload_fingerprints(self):
        self.fingerprints = []
        for fol, subs, files in os.walk(self.fingerprints_directory):
            for file in files:
                fp_path = os.path.join(fol, file)
                try:
                    with open(fp_path) as r:
                        fp = yaml.safe_load(r)
                    # Validate that correct things are in the fp yaml
                    if self.validate_fingerprint(fp):
                        self.fingerprints.append(fp)
                except Exception as e:
                    self.logger.error(str(e))

    def validate_fingerprint(self, fingerprint: Dict):
        assert "triggers" in fingerprint.keys()
        assert isinstance(fingerprint["triggers"], list)
        assert len(fingerprint["triggers"]) != 0

        assert "flows" in fingerprint.keys()
        assert isinstance(fingerprint["flows"], list)
        for dest in fingerprint["flows"]:
            assert "container_tag" in dest.keys()

        assert "destinations" in fingerprint.keys()
        assert isinstance(fingerprint["destinations"], list)
        for dest in fingerprint["destinations"]:
            assert "hostname" in dest.keys()
            assert "port" in dest.keys()
            assert "ae_title" in dest.keys()

        return True

