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
                flow_tar.addfile(member, storescp_tar.extractfile(member.name))
    file.seek(0)
    return file


def slice_dataframe_to_triggers(ds: pd.DataFrame, triggers: List):
    matches = []
    for trigger in triggers:
        match = ds

        for keyword, regex_patterns in trigger.items():
            if not keyword in match.columns:
                return False

            for regex_pattern in regex_patterns:
                if regex_pattern.startswith("~"):
                    match = match[~match[keyword].str.contains(regex_pattern[1:], regex=True, na=False)]  # Regex NOT match. This is "recursive"
                else:
                    match = match[match[keyword].str.contains(regex_pattern, regex=True, na=False)]  # Regex match. This is "recursive"
        matches.append(match)

    # Check for no matches
    for match in matches:
        if not bool(len(match)):
            return None

    # otherwise give it all back
    return pd.concat(matches)


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
