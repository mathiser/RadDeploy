from typing import Dict, List

from pydantic import BaseModel

ALLOWED_KWARGS: List = ["image", "command", "environment", "ports",
                        "cpu_period", 'cpu_quota', "cpu_rt_period", "cpu_rt_runtime",
                        "cpu_shares", "cpuset_cpus", "device_cgroup_rules",
                        "device_read_bps", "device_read_iops", "device_write_bps",
                        "device_write_iops", "entrypoint", "ipc_mode", "labels",
                        "mem_limit", "mem_reservation", "mem_swappiness",
                        "memswap_limit", "nano_cpus", "network_disabled",
                        "shm_size", "user"]

class Model(BaseModel):
    docker_kwargs: Dict

    gpu: str | bool
    input_dir: str
    output_dir: str

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.validate()

    def validate(self):
        assert "image" in self.docker_kwargs.keys()
        for key in self.docker_kwargs.keys():
            assert key in ALLOWED_KWARGS


class Destination(BaseModel):
    hostname: str
    port: int = 10000
    ae_title: str = 'STORESCP'


class FlowContext(BaseModel):
    triggers: List[Dict[str, str]]
    destinations: List[Destination] = []
    model: Model
    file_exchange: str | None = None
    file_queue: str | None = None
