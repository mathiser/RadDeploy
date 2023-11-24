import logging
import os
from typing import Dict

import yaml
from DicomFlowLib.contexts import SCPContext
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

    def fingerprint(self, context: SCPContext):
        self.reload_fingerprints()
        print(context)
    #     matching_series_instances = []
    #     triggers = [trigger for trigger in list(fp.triggers)]
    #     series_instances = [series_instance for _, series_instance in assoc.series_instances.items()]
    #     for trigger in triggers:
    #         for series_instance in series_instances:
    #             # Check all "in-patterns"
    #             if (trigger.sop_class_uid_exact is None or trigger.sop_class_uid_exact == series_instance.sop_class_uid) and \
    #                (trigger.series_description_pattern is None or trigger.series_description_pattern in series_instance.series_instance_uid) and \
    #                (trigger.study_description_pattern is None or trigger.study_description_pattern in series_instance.study_description):
    #                 # Check all "out-patterns"
    #                 if trigger.exclude_pattern is None or \
    #                     (series_instance.sop_class_uid is None or trigger.exclude_pattern not in series_instance.sop_class_uid) and \
    #                     (series_instance.series_instance_uid is None or trigger.exclude_pattern not in series_instance.series_instance_uid) and \
    #                     (series_instance.study_description is None or trigger.exclude_pattern not in series_instance.study_description):
    #                     matching_series_instances.append(series_instance)
    #                     break
    #         else:
    #             return None
    #
    #     return matching_series_instances
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

