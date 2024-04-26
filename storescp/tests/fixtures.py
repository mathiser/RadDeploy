import queue
import pytest
from storescp.src.scp import SCP


@pytest.fixture
def scp_out_queue():
    return queue.Queue()


@pytest.fixture
def scp(scp_out_queue):
    s = SCP(
        out_queue=scp_out_queue,
        port=10000,
        hostname="localhost",
        blacklist_networks=None,
        whitelist_networks=None,
        ae_title="RADDEPLOY",
        pynetdicom_log_level=20,
        log_level=10).start(blocking=False)
    yield s

    s.stop()