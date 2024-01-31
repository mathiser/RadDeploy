import os
from multiprocessing.pool import ThreadPool
from typing import List

import pandas as pd
import pydicom
import yaml

from DicomFlowLib.data_structures.flow import Flow


def parse_file_metas(file_metas: List) -> pd.DataFrame:
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


def fingerprint(ds: pd.DataFrame, triggers: List):
    for trigger in triggers:
        match = ds
        for keyword, regex_pattern in trigger.items():
            match = match[
                match[keyword].str.contains(regex_pattern, regex=True)]  # Regex match. This is "recursive"

        if not bool(len(match)):
            return False
    else:
        return True


def parse_fingerprints(flow_directory):
    for fol, subs, files in os.walk(flow_directory):
        for file in files:
            fp_path = os.path.join(fol, file)
            try:
                with open(fp_path) as r:
                    fp = yaml.safe_load(r)
                    yield Flow(**fp)

            except Exception as e:
                raise e
