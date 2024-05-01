import logging
import os
import subprocess
import threading
import time

import yaml
from RadDeployLib.test_utils.fixtures import *
import pytest

from main import Main

from RadDeployLib.data_structures.flow import Destination


@pytest.fixture
def config(tmpdir):
    with open(os.path.join(os.path.dirname(__file__), "..", "conf.d", "00_default_config.yaml"), "r") as r:
        config = yaml.safe_load(r)

    config["RABBIT_HOSTNAME"] = "localhost"
    config["RABBIT_PORT"] = 5677
    config["LOG_DIR"] = tmpdir
    config["LOG_LEVEL"] = 10
    config["AE_HOSTNAME"] = "localhost"
    config["FILE_STORAGE_URL"] = ""

    return config

@pytest.fixture
def main(config, fs):
    m = Main(config, fs)
    m.start(blocking=False)
    yield m
    m.stop()
@pytest.fixture
def test_scp():
    logging.info("Starting scp...")
    proc = subprocess.Popen(['python', "-m pynetdicom",  "storescp", "-aet RADDEPLOY", "--ignore", "-d", "11118"],
                            shell=True)
    yield proc
    time.sleep(3)
    logging.info("Terminating scp...")
    proc.terminate()


def test_main(config, mq_container, main, test_scp, fs, scp_tar):
    logging.getLogger().setLevel(10)
    uid = fs.post(scp_tar)
    assert uid
    for dest, status in main.scu.post_file_on_destinations(uid, [Destination(host="localhost", port=11118, ae_title="RADDEPLOY")]):
        logging.info(f"{status}, {str(dest)}")
        assert dest