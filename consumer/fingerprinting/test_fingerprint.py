import datetime
import os
import shutil
import tempfile
import unittest

from daemon.fingerprinting.fingerprint import fast_fingerprint, slow_fingerprint
from database.db import DB
from dicom_networking.scp import Assoc, SeriesInstance


class TestFingerprint(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = tempfile.mkdtemp()
        os.makedirs(self.tmp_dir, exist_ok=True)
        self.db = DB(base_dir=self.tmp_dir)
        self.series_instance1 = SeriesInstance(series_instance_uid="113.112.5553",
                                               study_description="Interesting Study",
                                               series_description="What a series",
                                               sop_class_uid="1.2.3",
                                               path="path/to/1")
        self.series_instance2 = SeriesInstance(series_instance_uid="112.112.112",
                                               study_description="A different but interesting Study",
                                               series_description="What anoter series",
                                               sop_class_uid="2.3.4",
                                               path="path/to/2")
        self.assoc = Assoc(assoc_id="1234567890",
                           timestamp=datetime.datetime.now(),
                           path="some/path",
                           series_instances={self.series_instance1.series_instance_uid: self.series_instance1,
                                             self.series_instance2.series_instance_uid: self.series_instance2})

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_dir)

    def get_fingerprint(self):
        return self.db.add_fingerprint(inference_server_url="http://test.com.org", human_readable_id="test")
    def test_fast_fingerprint_pass(self):
        fp = self.get_fingerprint()
        self.db.add_trigger(fingerprint_id=fp.id, sop_class_uid_exact="1.2.3")
        self.db.add_trigger(fingerprint_id=fp.id, sop_class_uid_exact="2.3.4")
        fp = self.db.get_fingerprint(fp.id)

        self.assertTrue(fast_fingerprint(fp=fp, assoc=self.assoc))

    def test_fast_fingerprint_fail(self):
        fp = self.get_fingerprint()
        self.db.add_trigger(fingerprint_id=fp.id, sop_class_uid_exact="1.2.3")
        self.db.add_trigger(fingerprint_id=fp.id, sop_class_uid_exact="2.3.5")
        fp = self.db.get_fingerprint(fp.id)
        self.assertFalse(fast_fingerprint(fp=fp, assoc=self.assoc))

    def test_slow_fingerprint_pass(self):
        fp = self.get_fingerprint()
        self.db.add_trigger(fingerprint_id=fp.id, sop_class_uid_exact="1.2.3", study_description_pattern="Interesting")
        self.db.add_trigger(fingerprint_id=fp.id, sop_class_uid_exact="2.3.4", exclude_pattern="BRAIN FART!!")
        fp = self.db.get_fingerprint(fp.id)
        matches = slow_fingerprint(fp=fp, assoc=self.assoc)
        self.assertIsNotNone(matches)
        self.assertEqual(2, len(matches))

    def test_slow_fingerprint_no_match(self):
        fp = self.get_fingerprint()
        self.db.add_trigger(fingerprint_id=fp.id, sop_class_uid_exact="1.2.3", study_description_pattern="Interesting")
        self.db.add_trigger(fingerprint_id=fp.id, sop_class_uid_exact="2.3.5", exclude_pattern="BRAIN FART!!")
        fp = self.db.get_fingerprint(fp.id)

        self.assertIsNone(slow_fingerprint(fp=fp, assoc=self.assoc))

    def test_slow_fingerprint_exclude(self):
        fp = self.get_fingerprint()
        self.db.add_trigger(fingerprint_id=fp.id, sop_class_uid_exact="1.2.3", series_description_pattern="What ")
        self.db.add_trigger(fingerprint_id=fp.id, sop_class_uid_exact="2.3.5", exclude_pattern="Interesting")
        fp = self.db.get_fingerprint(fp.id)
        matches = slow_fingerprint(fp=fp, assoc=self.assoc)
        self.assertIsNone(matches)


if __name__ == '__main__':
    unittest.main()
