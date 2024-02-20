import os
import tarfile
from io import BytesIO
from typing import List

import pandas as pd
import yaml

from DicomFlowLib.data_structures.flow import Flow


def generate_flow_specific_tar(tar_file: BytesIO, sliced_df: pd.DataFrame):
    tar_file.seek(0)
    file = BytesIO()

    with tarfile.TarFile.open(fileobj=tar_file, mode="r|*") as storescp_tar:
        flow_tar = tarfile.TarFile.open(fileobj=file, mode="w")
        for member in flow_tar.getmembers():
            if member.name in sliced_df["dcm_path"]:
                print(member)
                flow_tar.addfile(member, storescp_tar.extractfile(member.name))
    file.seek(0)
    return file


def slice_dataframe_to_triggers(ds: pd.DataFrame, triggers: List):
    match = None
    for trigger in triggers:
        match = ds

        for keyword, regex_patterns in trigger.items():
            if not keyword in match.columns:
                return False

            for regex_pattern in regex_patterns:
                if regex_pattern.startswith("~"):
                    exclude = match[match[keyword].str.contains(regex_pattern[1:], regex=True,
                                                                na=False)]  # Regex NOT match. This is "recursive"
                    if bool(len(exclude)):
                        return None
                else:
                    match = match[match[keyword].str.contains(regex_pattern, regex=True,
                                                              na=False)]  # Regex match. This is "recursive"

    return match


def parse_fingerprints(flow_directory: str):
    for fol, subs, files in os.walk(flow_directory):
        for file in files:
            if not file.endswith("yaml"):
                continue
            fp_path = os.path.join(fol, file)
            try:
                with open(fp_path) as r:
                    fp = yaml.safe_load(r)
                    yield Flow(**fp)
            except Exception as e:
                raise e
