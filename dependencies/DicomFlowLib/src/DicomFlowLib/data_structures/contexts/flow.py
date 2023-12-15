from dataclasses import dataclass, field
from typing import Dict, List

from dataclass_wizard import JSONWizard

ALLOWED_KWARGS: List = ["image", "command", "environment", "ports",
                        "cpu_period", 'cpu_quota', "cpu_rt_period", "cpu_rt_runtime",
                        "cpu_shares", "cpuset_cpus", "device_cgroup_rules",
                        "device_read_bps", "device_read_iops", "device_write_bps",
                        "device_write_iops", "entrypoint", "ipc_mode", "labels",
                        "mem_limit", "mem_reservation", "mem_swappiness",
                        "memswap_limit", "nano_cpus", "network_disabled",
                        "shm_size", "user"]


@dataclass
class Model(JSONWizard):
    dockerKwargs: Dict

    gpu: str | bool
    inputDir: str | None = None
    outputDir: str | None = None


    def validate(self):
        assert "image" in self.dockerKwargs.keys()
        for key in self.dockerKwargs.keys():
            assert key in ALLOWED_KWARGS


@dataclass
class Destination(JSONWizard):
    hostname: str
    port: int = 10000
    aeTitle: str = 'STORESCP'


@dataclass
class FlowContext(JSONWizard):
    model: Model
    destinations: List[Destination] = field(default_factory=list)
    triggers: List[Dict[str, str]] = field(default_factory=list)

