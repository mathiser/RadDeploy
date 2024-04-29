import json
import time
from typing import List

from sqlalchemy import ForeignKey, PickleType, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, validates

from RadDeployLib.data_structures.flow import Model, Flow
from RadDeployLib.data_structures.service_contexts import FlowContext


class Base(DeclarativeBase):
    id: Mapped[int] = mapped_column(primary_key=True, unique=True, autoincrement=True)
    ts: Mapped[str] = mapped_column(default=time.time)


class DBMountMapping(Base):
    __tablename__ = "db_mount_mappings"
    db_flow_id: Mapped[int] = mapped_column(ForeignKey('db_flows.id'))
    src: Mapped[str]
    uid: Mapped[str]
    __table_args__ = (UniqueConstraint('db_flow_id', 'src', name='_mount_mapping_uc'),)


class DBJob(Base):
    __tablename__ = "db_jobs"
    # Static variables
    db_flow_id: Mapped[int] = mapped_column(ForeignKey("db_flows.id"))
    db_flow: Mapped["DBFlow"] = relationship("DBFlow",
                                             back_populates="db_jobs",
                                             uselist=False,
                                             lazy="joined",
                                             join_depth=2)
    model_json: Mapped[str]

    # Dynamic variables
    pub_model_routing_key: Mapped[str]
    priority: Mapped[int]
    retries: Mapped[int] = mapped_column(default=0)

    # 0: Pending,
    # 1: Dispatched
    # 2: Timeout
    # 200 success
    # 400 = fail
    status: Mapped[int] = mapped_column(default=0)

    @validates("priority")
    def validate_priority(self, key, priority):
        if 0 <= priority <= 5:
            return priority
        elif 5 < priority:
            return 5
        elif priority < 0:
            return 0
        else:
            raise ValueError("Imaginary number?!")

    @property
    def model(self):
        return Model(**json.loads(self.model_json))

    @property
    def timeout(self):
        return self.model.timeout

    @property
    def input_mount_keys(self):
        return set(self.model.input_mount_keys)

    @property
    def output_mount_keys(self):
        return set(self.model.output_mount_keys)

    def has_input_mounts_available(self) -> bool:
        return set(self.input_mount_keys).issubset(set(self.db_flow.mount_mapping.keys()))

    def is_finished(self) -> bool:
        return set(self.output_mount_keys).issubset(set(self.db_flow.mount_mapping.keys()))

    def is_runnable(self):
        return self.has_input_mounts_available() and not self.is_finished()


class DBFlow(Base):
    __tablename__ = "db_flows"
    flow_context_json: Mapped[str]
    db_jobs: Mapped[List[DBJob]] = relationship("DBJob",
                                                back_populates="db_flow",
                                                lazy="joined", join_depth=2)
    _mount_mappings: Mapped[List[DBMountMapping]] = relationship("DBMountMapping",
                                                                 lazy="joined",
                                                                 join_depth=2)
    # 0: Pending,
    # 1: Dispatched
    # 2: Timeout
    # 200 success
    # 400 = fail
    status: Mapped[int] = mapped_column(default=0)


    @property
    def mount_mapping(self):
        return {mm.src: mm.uid for mm in self._mount_mappings}

    @property
    def flow_context(self):
        return FlowContext(**json.loads(self.flow_context_json))

    def is_finished(self):
        for db_job in self.db_jobs:
            if not db_job.is_finished():
                return False
        else:
            return True

    def update_status(self):
        for db_job in self.db_jobs:
            if not db_job.is_finished():
                return False
        else:
            return True
