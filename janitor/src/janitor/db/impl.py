import os

import sqlalchemy
from sqlalchemy.orm import sessionmaker, scoped_session

from DicomFlowLib.data_structures.contexts import FlowContext
from DicomFlowLib.fs import FileStorage
from .db_models import Base, Event


class Database:
    def __init__(self, database_path: str, file_storage: FileStorage):
        self.fs = file_storage
        self.database_path = database_path
        os.makedirs(os.path.dirname(self.database_path), exist_ok=True)

        self.database_url = f'sqlite:///{self.database_path}'
        self.engine = sqlalchemy.create_engine(self.database_url, future=True)

        # Check if database exists - if not, create scheme
        if not os.path.isfile(self.database_path):
            Base.metadata.create_all(self.engine)

        self.session_maker = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.Session = scoped_session(self.session_maker)

    def add_event(self,
                  exchange: str,
                  routing_key: str,
                  context: FlowContext):
        with self.Session() as session:
            event = Event(uid=context.uid,
                          flow_instance_uid=context.flow_instance_uid,
                          exchange=exchange,
                          routing_key=routing_key,
                          context_as_json=context.model_dump_json(exclude={"file_metas"}),
                          flow_name=context.flow.name if context.flow else None,
                          input_file_uid=context.input_file_uid,
                          output_file_uid=context.output_file_uid,
                          sender_ae_hostname=context.sender.host,
                          sender_ae_title=context.sender.ae_title,
                          sender_ae_port=context.sender.port)
            session.add(event)
            session.commit()
            session.refresh(event)

            return event

    def update_event(self, _id, **kwargs):
        with self.Session() as session:
            event = session.query(Event).filter_by(id=_id).first()
            for k, v in kwargs.items():
                event.__setattr__(k, v)
            session.commit()
            session.refresh(event)
        return event

    def get_events_by_kwargs(self, **kwargs):
        with self.Session() as session:
            return session.query(Event).filter_by(**kwargs)

    def delete_files_by_id(self, _id):
        event = self.get_events_by_kwargs(id=_id).first()
        try:
            if not event.input_file_deleted:
                self.fs.delete(event.input_file_uid)
                self.update_event(_id=event.id, input_file_deleted=True)
        except FileNotFoundError:
            self.update_event(_id=event.id, input_file_deleted=True)
        except Exception as e:
            raise e

        ## Output file
        try:
            if event.output_file_uid:
                if not event.output_file_deleted:
                    self.fs.delete(event.output_file_uid)
                    self.update_event(_id=event.id, output_file_deleted=True)
        except FileNotFoundError:
            self.update_event(_id=event.id, output_file_deleted=True)
        except Exception as e:
            raise e

    def delete_all_files_by_kwargs(self, **kwargs):
        all_events_by_uid = self.get_events_by_kwargs(**kwargs).all()
        for event in all_events_by_uid:
            try:
                self.delete_files_by_id(_id=event.id)
            except FileNotFoundError:
                pass

