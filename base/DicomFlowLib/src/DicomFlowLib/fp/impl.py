import json
import os
import traceback
from multiprocessing.pool import ThreadPool
from typing import List, Iterable

import pandas as pd
import pydicom
import yaml

from DicomFlowLib.data_structures.contexts import FlowContext
from DicomFlowLib.data_structures.flow import Flow
from DicomFlowLib.data_structures.mq import MQEntrypointResult
from DicomFlowLib.fs import FileStorageClient
from DicomFlowLib.log import CollectiveLogger


class Fingerprinter:
    def __init__(self,
                 file_storage: FileStorageClient,
                 logger: CollectiveLogger,
                 flow_directory: str):
        self.logger = logger
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
                self.logger.debug(f"FOUND NOT MATCH", uid=self.uid, finished=True)
                return False
        else:
            self.logger.info(f"FOUND MATCH", uid=self.uid, finished=True)
            return True

    def mq_entrypoint(self, basic_deliver, body) -> Iterable[MQEntrypointResult]:
        results = []
        org_context = FlowContext(**json.loads(body.decode()))
        self.uid = org_context.uid

        ds = self.parse_file_metas(org_context.file_metas)

        for flow in self.parse_fingerprints():
            if self.fingerprint(ds, flow.triggers):
                self.logger.info(f"MATCHING FLOW", uid=self.uid, finished=True)
                context = org_context.model_copy(deep=True)
                context.add_flow(flow.model_copy(deep=True))

                # Provide a link to the input file
                context.input_file_uid = self.fs.clone(context.input_file_uid)

                # Publish the
                results.append(MQEntrypointResult(body=context.model_dump_json().encode(),
                                                  priority=flow.priority))

            else:
                self.logger.info(f"NOT MATCHING FLOW", uid=self.uid, finished=True)
        self.uid = None
        return results

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
                    raise e
