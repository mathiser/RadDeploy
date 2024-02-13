import logging
import os

import sqlalchemy
from sqlalchemy.orm import sessionmaker, scoped_session

from .db_models import Base, DispatchedModel, MountMapping


class Database:
    def __init__(self, database_path: str, log_level: int = str):
        self.database_path = database_path
        try:
            os.makedirs(os.path.dirname(self.database_path), exist_ok=True)
        except FileNotFoundError:
            pass
        self.database_url = f'sqlite:///{self.database_path}'
        self.engine = sqlalchemy.create_engine(self.database_url, future=True)

        # Check if database exists - if not, create scheme
        if not os.path.isfile(self.database_path):
            Base.metadata.create_all(self.engine)

        self.session_maker = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.Session = scoped_session(self.session_maker)

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)

    def insert_mount_mapping(self,
                             uid: str,
                             name: str,
                             file_uid: str):

        with self.Session() as session:
            try:
                row = MountMapping(uid=uid,
                                   name=name,
                                   file_uid=file_uid)
                session.add(row)
                session.commit()
                session.refresh(row)
                return row
            except Exception as e:
                return None

    def insert_dispatched_model(self,
                                uid: str,
                                model_id: int):
        with self.Session() as session:
            try:
                row = DispatchedModel(uid=uid,
                                      model_id=model_id)
                session.add(row)
                session.commit()
                session.refresh(row)
                return row
            except Exception as e:
                return None

    def get_objs_by_kwargs(self, _obj_type,  **kwargs):
        with self.Session() as session:
            return session.query(_obj_type).filter_by(**kwargs)

    def delete_all_by_uid(self, uid):
        self.get_objs_by_kwargs(MountMapping, uid=uid).delete()
        self.get_objs_by_kwargs(DispatchedModel, uid=uid).delete()