import os
import unittest

import pandas as pd
import pydicom

from DicomFlowLib.data_structures.flow import Flow
from ..fp_utils import parse_file_metas, fingerprint, parse_fingerprints

class TestFP(unittest.TestCase):
    def test_parse_file_metas(self):
        ds = pydicom.dcmread("fp/tests/test_data/scans/mr.dcm")
        df = parse_file_metas([ds.to_json()])
        self.assertEqual(type(df), pd.DataFrame)

    def test_parse_fingerprints(self):
        flows = parse_fingerprints("fp/tests/test_data/flows.bak/")
        for flow in flows:
            self.assertEqual(type(flow), Flow)

    def test_fingerprint(self):
        file_metas = []
        for fol, subs, files in os.walk("fp/tests/test_data/scans"):
            for file in files:
                ds = pydicom.dcmread(os.path.join(fol, file)).to_json()
                file_metas.append(ds)

        ds = parse_file_metas(file_metas)
        flows = parse_fingerprints("fp/tests/test_data/flows.bak")
        for flow in flows:
            self.assertEqual(flow.optional_kwargs["pass"], fingerprint(ds, flow.triggers))



if __name__ == '__main__':
    unittest.main()
