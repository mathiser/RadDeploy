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

    input_dir: str | None = None
    output_dir: str | None = None

    static_mounts: List[str] = []

    pull_before_exec: bool = True
    timeout: int = 1800

    def validate_docker_kwargs(self):
        assert "image" in self.docker_kwargs.keys()
        for key in self.docker_kwargs.keys():
            assert key in ALLOWED_KWARGS