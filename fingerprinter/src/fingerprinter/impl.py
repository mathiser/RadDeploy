import json
import os
import traceback
from multiprocessing.pool import ThreadPool
from typing import List

import pandas as pd
import pydicom
import yaml

from DicomFlowLib.data_structures.contexts import FlowContext, PubModel
from DicomFlowLib.data_structures.flow import Flow
from DicomFlowLib.fs import FileStorage
from DicomFlowLib.log import CollectiveLogger
from DicomFlowLib.mq import MQBase, MQSubEntrypoint


class Fingerprinter(MQSubEntrypoint):
    def __init__(self, pub_models: List[PubModel], file_storage: FileStorage, logger: CollectiveLogger, flow_directory: str):
        super().__init__(logger, pub_models)

        self.pub_declared = False
        self.flows = []
        self.flow_directory = flow_directory
        self.fs = file_storage
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
        for trigger in triggers:
            match = ds
            for keyword, regex_pattern in trigger.items():
                match = match[
                    match[keyword].str.contains(regex_pattern, regex=True)]  # Regex match. This is "recursive"

            if not bool(len(match)):
                self.logger.info(f"FOUND NOT MATCH", uid=self.uid, finished=True)
                return False

        else:
            self.logger.info(f"FOUND MATCH", uid=self.uid, finished=True)
            self.logger.info(f"RUNNING FINGERPRINTING", uid=self.uid, finished=True)
            return True

    def mq_entrypoint(self, connection, channel, basic_deliver, properties, body):

        context = FlowContext(**json.loads(body.decode()))
        self.uid = context.uid
        mq = MQBase(logger=self.logger, close_conn_on_exit=False).connect_with(connection=connection, channel=channel)

        ds = self.parse_file_metas(context.file_metas)

        for flow in self.parse_fingerprints():
            if self.fingerprint(ds, flow.triggers):
                self.logger.info(f"MATCHING FLOW", uid=self.uid, finished=True)
                context.flow = flow.model_copy()

                # Provide a link to the input file
                context.input_file_uid = self.fs.clone(context.input_file_uid)

                # Publish the
                self.publish(mq, context)

            else:
                self.logger.info(f"NOT MATCHING FLOW", uid=self.uid, finished=True)

        self.uid = None

    def parse_fingerprints(self):
        for fol, subs, files in os.walk(self.flow_directory):
            for file in files:
                fp_path = os.path.join(fol, file)
                try:
                    with open(fp_path) as r:
                        fp = yaml.safe_load(r)
                        yield Flow(**fp)

                except Exception as e:
                    self.logger.error(f"Error when parsing flow definition: {fp_path} - skipping")
                    self.logger.error(str(e))
                    self.logger.error(traceback.format_exc())
