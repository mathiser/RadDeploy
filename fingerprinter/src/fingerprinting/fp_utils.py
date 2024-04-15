import os
import tarfile
import tempfile
from io import BytesIO
from typing import List, Dict

import pandas as pd
import pydicom
import yaml

from DicomFlowLib.data_structures.flow import Flow


def generate_df_from_tar(tar_file: BytesIO) -> pd.DataFrame:
    tar_file.seek(0)
    df = pd.DataFrame()
    with tarfile.TarFile.open(fileobj=tar_file) as tf, tempfile.TemporaryDirectory() as tmp_dir:
        tf.extractall(tmp_dir, filter='data')
        for fol, subs, files in os.walk(tmp_dir):
            for file in files:
                p = os.path.join(fol, file)
                ds = pydicom.dcmread(p, force=True, stop_before_pixels=True)
                elems = {"path": p.replace(tmp_dir, "").strip("/")}
                for elem in ds:
                    if elem.keyword == "PixelData":
                        continue
                    else:
                        elems[str(elem.keyword)] = str(elem.value)

                df = pd.concat([df, pd.DataFrame([elems])], ignore_index=True)
    tar_file.seek(0)
    return df


def generate_sub_tar_file_path(row: Dict, tar_subdir: List):
    prefix = [row[c] if (c in row.keys()) else c for c in tar_subdir]
    return os.path.join(*prefix,
                        "_".join([row["Modality"], row["SeriesInstanceUID"], row["SOPInstanceUID"] + ".dcm"]))


def generate_flow_specific_tar(tar_file: BytesIO, sliced_df: pd.DataFrame, tar_subdir: List | None = None) -> BytesIO:
    tar_file.seek(0)

    if not tar_subdir:
        tar_subdir = []

    tar_file.seek(0)
    file = BytesIO()
    with tarfile.TarFile.open(fileobj=tar_file, mode="r") as storescp_tar:
        flow_tar = tarfile.TarFile.open(fileobj=file, mode="w")
        for member in storescp_tar.getmembers():
            rows = sliced_df[sliced_df["path"] == member.name]
            if len(rows) == 1:
                info = tarfile.TarInfo(
                    generate_sub_tar_file_path(rows.iloc[0], tar_subdir))  # Should not be able to get more than one.)
                info.size = member.size
                flow_tar.addfile(info, storescp_tar.extractfile(member.name))
            elif len(rows) == 0:
                pass
            else:
                raise Exception("Unexpected number of rows matched - possible STORESCP overwrite files unintentionally")
    file.seek(0)
    return file


def slice_dataframe_to_triggers(ds: pd.DataFrame, triggers: List):
    matches = []
    for col in ds.columns:
        ds[col] = ds[col].astype(str)

    for trigger in triggers:
        match = ds

        for keyword, regex_patterns in trigger.items():
            if not keyword in match.columns:
                return None
            ds[keyword] = ds[keyword].astype(str)
            for regex_pattern in regex_patterns:
                if regex_pattern.startswith("~"):
                    match = match[~match[keyword].str.contains(regex_pattern[1:], regex=True,
                                                               na=False)]  # Regex NOT match. This is "recursive"
                else:
                    match = match[match[keyword].str.contains(regex_pattern, regex=True,
                                                              na=False)]  # Regex match. This is "recursive"
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
