from typing import Dict, List, Any

import networkx
import yaml
from pydantic import BaseModel, Field

from .model import Model
from .destination import Destination


class Flow(BaseModel):
    name: str = ""
    version: str = ""
    priority: int = Field(default=0, ge=0, le=4, description="Priority must be in the range 0-4")
    triggers: List[Dict[str, List[str]]] = []
    models: List[Model] = []
    destinations: List[Destination] = []
    return_to_sender_on_ports: List[int] = []
    extra: Dict = {}
    tar_subdir: List[str] = []

    def __init__(self, **data: Any):
        super().__init__(**data)
        assert self.is_valid_dag(self.models)

    @staticmethod
    def from_file(path):
        with open(path) as r:
            return Flow(**yaml.safe_load(r))

    @staticmethod
    def is_valid_dag(models):
        inputs = set()
        outputs = set()
        G = networkx.MultiDiGraph()
        G.add_nodes_from([i for i, m in enumerate(models)])

        # Construct a networkx graph to evaluate validity.
        for i in range(len(models)):
            outputs_i = models[i].output_mount_keys
            outputs = outputs.union(outputs_i)

            for u in range(len(models)):
                inputs_u = models[u].input_mount_keys
                inputs = inputs.union(inputs_u)

                edges = outputs_i.intersection(inputs_u)
                for e in edges:
                    G.add_edge(i, u, name=e)

        if not "src" in inputs:
            raise Exception("'src' must be provided at least once in inputs")
        if "src" in outputs:
            raise Exception("'src' cannot be used as output - use for inputs only")
        if not networkx.is_directed_acyclic_graph(G):
            raise Exception("is not directed and acyclic")

        return True