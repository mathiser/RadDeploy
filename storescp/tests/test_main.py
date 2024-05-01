import os
import time

import pytest
import yaml
from main import Main

from RadDeployLib.test_utils.fixtures import mq_container, mq_base, scan_dir, scp_tar_path, fs


@pytest.fixture
def config(tmpdir, mq_container):
    with open(os.path.join(os.path.dirname(__file__), "..", "conf.d", "00_default_config.yaml"), "r") as r:
        config = yaml.safe_load(r)

    config["RABBIT_HOSTNAME"] = "localhost"
    config["RABBIT_PORT"] = 5677
    config["LOG_DIR"] = tmpdir
    config["LOG_LEVEL"] = 10
    config["AE_HOSTNAME"] = "localhost"

    return config

@pytest.fixture
def main(config, fs):
    m = Main(config, file_storage=fs)
    m.start(blocking=False)
    yield m
    m.stop()

def test_main(main, mq_container, config, mq_base, scan_dir, scp_tar_path):
    exchange_name = config["PUB_MODELS"][0]["exchange"]
    mq_base.setup_exchange(exchange_name, "topic")
    q_out = mq_base.setup_queue_and_bind(exchange_name, "#")
    os.system(f"python -m pynetdicom storescu -r -v {config["AE_HOSTNAME"]} {config["AE_PORT"]} {scan_dir}")

    time.sleep(1)

    method_frame, header_frame, body = mq_base._channel.basic_get(q_out, auto_ack=True)
    assert method_frame
