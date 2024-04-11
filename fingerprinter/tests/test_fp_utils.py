from io import BytesIO

import pytest
from DicomFlowLib.data_structures.flow import Flow
from fingerprinter.src.fingerprinter.fp_utils import parse_fingerprints, generate_df_from_tar, \
    generate_flow_specific_tar, slice_dataframe_to_triggers


@pytest.fixture
def flow_dir():
    return "tests/test_data/flows/"


@pytest.fixture
def scp_tar():
    with open("tests/test_data/scans/scp.tar", "rb") as r:
        return BytesIO(r.read())


def test_parse_fingerprints(flow_dir):
    flows = parse_fingerprints(flow_dir)
    for flow in flows:
        isinstance(flow, Flow)


def test_generate_df_from_tar(scp_tar):
    df = generate_df_from_tar(scp_tar)
    assert "path" in df.columns
    assert df["path"].tolist() == ['ct.dcm', 'mr.dcm']


def test_generate_flow_specific_tar(scp_tar):
    df = generate_df_from_tar(scp_tar)
    sliced_tar = generate_flow_specific_tar(tar_file=scp_tar, sliced_df=df[df["Modality"] == "MR"])
    sliced_df = generate_df_from_tar(sliced_tar)
    assert sliced_df["path"].tolist() == [
        "MR_1.3.46.670589.11.71891.5.0.8000.2022062210324747248_1.3.46.670589.11.71891.5.0.8000.2022062210385801250.dcm"]

    sliced_tar = generate_flow_specific_tar(tar_file=scp_tar, sliced_df=df[df["Modality"] == ["CT", "MR"]])
    sliced_df = generate_df_from_tar(sliced_tar)
    assert sliced_df["path"].tolist() == [
        'CT_1.3.6.1.4.1.40744.29.33371661027192187491509798061184654147_1.3.6.1.4.1.40744.29.179461095576983824275091091253440398486.dcm',
        "MR_1.3.46.670589.11.71891.5.0.8000.2022062210324747248_1.3.46.670589.11.71891.5.0.8000.2022062210385801250.dcm"]


def test_slice_dataframe_to_triggers(scp_tar):
    ct_trigger = [
        {"Modality": ["CT"]}
    ]
    df = generate_df_from_tar(scp_tar)
    sliced_df = slice_dataframe_to_triggers(triggers=ct_trigger, ds=df)

    sliced_tar = generate_flow_specific_tar(tar_file=scp_tar, sliced_df=sliced_df)
    sliced_tar_df = generate_df_from_tar(sliced_tar)
    assert set(sliced_tar_df["SeriesInstanceUID"].tolist()) == set(sliced_df["SeriesInstanceUID"].tolist())


if __name__ == '__main__':
    pytest.main()
