import time
from io import BytesIO

import pytest

from file_manager import FileManager


@pytest.fixture
def temp_bytes_io():
    return BytesIO(b"Hello World")


def test_file_manager_non_deleting(tmp_path, temp_bytes_io):
    fm = FileManager(base_dir=tmp_path)

    # Post
    uid = fm.post_file(temp_bytes_io)
    assert uid
    # Exist
    assert fm.file_exists(uid)

    # Clone
    clone_uid = fm.clone_file(uid)
    assert uid != clone_uid
    assert fm.file_exists(clone_uid)

    # Hash
    assert fm.get_hash(uid) == fm.get_hash(clone_uid)

    # Delete
    fm.delete_file(uid)
    assert not fm.file_exists(uid)
    fm.delete_file(clone_uid)
    assert not fm.file_exists(clone_uid)


def test_file_manager_deleting(tmp_path, temp_bytes_io):
    fm = FileManager(base_dir=tmp_path,
                     delete_run_interval=1,
                     delete_files_after=2,
                     log_level=10)
    t0 = time.time()
    uid = fm.post_file(temp_bytes_io)
    counter = 0
    while counter < 5:
        time.sleep(1)
        counter += 1
        if not fm.file_exists(uid):
            if time.time() - t0 > 2:
                assert True
            else:
                raise Exception("Deleted before it was meant to")





if __name__ == "__main__":
    pytest.main()
