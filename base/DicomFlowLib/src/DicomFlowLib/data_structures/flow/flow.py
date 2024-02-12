from typing import Dict, List, Any

import networkx
import yaml
from pydantic import BaseModel, Field

from DicomFlowLib.data_structures.flow.model import Model
from DicomFlowLib.data_structures.flow.destination import Destination


class Flow(BaseModel):
    name: str = ""
    version: str = ""
    priority: int = Field(default=0, ge=0, le=4, description="Priority must be in the range 0-4")
    triggers: List[Dict[str, List[str]]] = []

    models: List[Model] = []

    destinations: List[Destination] = []

    return_to_sender_on_ports: List[int] = []
    extra: Dict = {}

    def __init__(self, **data: Any):
        super().__init__(**data)
        assert self.is_valid_dag()

    def is_valid_dag(self):
        inputs = set()
        outputs = set()
        all_output_mounts = []
        G = networkx.MultiDiGraph()
        G.add_nodes_from([i for i, m in enumerate(self.models)])

        for i in range(len(self.models)):
            all_output_mounts += self.models[i].output_mounts.keys()
            for u in range(len(self.models)):
                outputs_i = set(self.models[i].output_mounts.keys())
                outputs = outputs.union(outputs_i)
                inputs_u = set(self.models[u].input_mounts.keys())
                inputs = inputs.union(inputs_u)

                edges = outputs_i.intersection(inputs_u)
                for e in edges:
                    G.add_edge(i, u, name=e)
        try:
            if all_output_mounts.count("dst") != 1:
                raise Exception(f"'dst' must be used only once! Found outputs: {all_output_mounts}")
            if "dst" in inputs:
                raise Exception("'dst' may not be used as input - use only for outputs only")
            if "src" in outputs:
                raise Exception("'src' may not be used output - use only for inputs only")
            if not networkx.is_directed_acyclic_graph(G):
                raise Exception("is not directed and acyclic")
            if not inputs.symmetric_difference(outputs) == {"src", "dst"}:
                resid = inputs.symmetric_difference(outputs)
                resid.remove("src")
                resid.remove("dst")
                raise Exception(f"Invalid mapping - don't know what to do with {resid}")
        except Exception as e:
            raise e
        return True

if __name__ == "__main__":
    file = "DicomFlowLib/data_structures/flow/tests/test_flows/dag_flow.yaml"
    with open(file) as r:
        fp = yaml.safe_load(r)
    flow = Flow(**fp)
    print(flow)
    flow.is_valid_dag()