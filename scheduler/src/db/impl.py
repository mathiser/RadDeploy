import logging
import os
from typing import List, Type, Dict

import sqlalchemy
from sqlalchemy.orm import sessionmaker, scoped_session
from RadDeployLib.data_structures.flow import Model
from RadDeployLib.data_structures.service_contexts import FlowContext
from .db_models import Base, DBFlow, DBJob, DBMountMapping


class Database:
    def __init__(self, database_path: str, declarative_base=Base, log_level=10):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        self.declarative_base = declarative_base

        self.database_path = database_path
        os.makedirs(os.path.dirname(self.database_path), exist_ok=True)

        self.database_url = f'sqlite:///{self.database_path}'
        self.engine = sqlalchemy.create_engine(self.database_url, future=True)

        # Check if database exists - if not, create scheme
        if not os.path.exists(self.database_path):
            self.declarative_base.metadata.create_all(self.engine)

        self.session_maker = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.Session = scoped_session(self.session_maker)

    def add(self, obj):
        with self.Session() as session:
            session.add(obj)
            session.commit()
            session.refresh(obj)
            return obj

    def add_db_flow(self,
                    flow_context: FlowContext,
                    UID,
                    Name,
                    Version,
                    Priority,
                    Destinations,
                    Sender) -> DBFlow:
        db_flow = DBFlow(
            flow_context_json=flow_context.model_dump_json(),
            UID=UID,
            Name=Name,
            Version=Version,
            Priority=Priority,
            Destinations=Destinations,
            Sender=Sender
        )
        return self.add(db_flow)

    def add_job(self,
                db_flow_id: int,
                pub_model_routing_key: str,
                priority: int,
                model: Model) -> DBJob:
        db_model = DBJob(
            db_flow_id=db_flow_id,
            model_json=model.model_dump_json(),
            pub_model_routing_key=pub_model_routing_key,
            priority=priority,

        )
        return self.add(db_model)

    def add_mount_mapping(self, db_flow_id: int, src: str, uid: str) -> DBMountMapping:
        db_mount_mapping = DBMountMapping(
            src=src,
            uid=uid,
            db_flow_id=db_flow_id
        )
        return self.add(db_mount_mapping)


    def get_by_kwargs(self, cls: Type[Base], where_kwargs: Dict, first: bool = False):
        with self.Session() as session:
            if first:
                return session.query(cls).filter_by(**where_kwargs).first()
            else:
                return session.query(cls).filter_by(**where_kwargs).all()

    def get_all(self, cls) -> List:
        with self.Session() as session:
            return session.query(cls).all()

    def update_by_kwargs(self, cls, where_kwargs: Dict, update_kwargs: Dict, first: bool = False):
        objs = []
        with self.Session() as session:
            for obj in session.query(cls).filter_by(**where_kwargs):
                for k, v in update_kwargs.items():
                    obj.__setattr__(k, v)

                session.commit()
                session.refresh(obj)
                if first:
                    return obj
                else:
                   objs.append(obj)
        return objs
