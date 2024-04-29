from abc import ABC, abstractmethod
from typing import List, Dict, Iterable

from RadDeployLib.data_structures.flow import Flow, Model
from RadDeployLib.data_structures.service_contexts import FlowContext
from RadDeployLib.data_structures.service_contexts.models import JobContext
from .db_models import DBMountMapping, DBFlow


class DBInterface(ABC):
    @abstractmethod
    def add_db_flow(self, flow_context: FlowContext) -> DBFlow:
        pass

    @abstractmethod
    def add_db_job_context(self,
                             flow_id: int,
                             pub_model_routing_key: str,
                             priority: int,
                             model: Model) -> DBFlow:
        pass

    @abstractmethod
    def add_mount_mapping(self, flow_id: int, src: str, uid: str) -> DBMountMapping:
        pass

    @abstractmethod
    def get_all_by_kwargs(self, cls, kwargs) -> List:
        pass

    @abstractmethod
    def get_all(self, cls) -> List:
        pass

    @abstractmethod
    def update_by_kwargs(self, cls, where_kwargs: Dict, update_kwargs: Dict) -> Iterable:
        pass