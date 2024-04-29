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
    gpu: str | bool = False
    input_mounts: Dict[str, str] = {"src": "/input"}
    output_mounts: Dict[str, str] = {"dst": "/output"}
    static_mounts: Dict[str, str] = {}

    pull_before_exec: bool = True
    timeout: int = 1800

    config_path: str = "/config/config.yaml"
    config: Dict = {}

    def validate_docker_kwargs(self):
        assert "image" in self.docker_kwargs.keys()
        for key in self.docker_kwargs.keys():
            assert key in ALLOWED_KWARGS

    @property
    def input_mount_keys(self):
        return set(self.input_mounts.keys())

    @property
    def output_mount_keys(self):
        return set(self.output_mounts.keys())

    @property
    def static_mount_keys(self):
        return set(self.static_mounts.keys())

    def remap_input_mount_keys(self, mapping: Dict):  # Mapping must be {"src": "qwer-qwer-qwer-(UID)"}
        for name, dst in self.input_mounts.items():
            yield {
                "src": name,
                "dst": dst,
                "uid": mapping[name]
            }
