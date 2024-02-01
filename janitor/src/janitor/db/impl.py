import os

import sqlalchemy
from sqlalchemy.orm import sessionmaker, scoped_session

from DicomFlowLib.data_structures.contexts import FlowContext
from DicomFlowLib.fs import FileStorageClient
from DicomFlowLib.log import CollectiveLogger
from .db_models import Base, Event


class Database:
    def __init__(self, logger: CollectiveLogger, database_path: str, file_storage: FileStorageClient):
        self.fs = file_storage
        self.logger = logger
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
                          input_file_uid=context.input_file_uid,
                          output_file_uid=context.output_file_uid)
            session.add(event)
            session.commit()
            session.refresh(event)

            return event

    def update_event(self, id, **kwargs):
        with self.Session() as session:
            event = session.query(Event).filter_by(id=id).first()
            for k, v in kwargs.items():
                event.__setattr__(k, v)
            session.commit()
            session.refresh(event)
        return event

    def get_objs_by_kwargs(self, **kwargs):
        with self.Session() as session:
            return session.query(Event).filter_by(**kwargs)

    def delete_input_files_by_kwargs(self, **kwargs):
        events = self.get_objs_by_kwargs(**kwargs).all()
        for event in events:
            if not event.input_file_deleted:
                try:
                    self.logger.info(f"Deleted files successfully for event with id: {event.id}")
                    self.fs.delete(event.input_file_uid)
                    self.update_event(id=event.id, input_file_deleted=True)
                except FileNotFoundError:
                    self.logger.info(f"File already deleted for event with id: {event.id}")
                    self.update_event(id=event.id, input_file_deleted=True)
                except Exception as e:
                    self.logger.error(str(e))
                    raise e

    def delete_output_files_by_kwargs(self, **kwargs):
        events = self.get_objs_by_kwargs(**kwargs).all()
        for event in events:
            if event.output_file_uid != "":
                if not event.output_file_deleted:
                    try:
                        self.fs.delete(event.output_file_uid)
                        self.update_event(id=event.id, output_file_deleted=True)
                        self.logger.info(f"Deleted files successfully for event with id: {event.id}")
                    except FileNotFoundError:
                        self.update_event(id=event.id, output_file_deleted=True)
                        self.logger.info(f"File already deleted for event with id: {event.id}")
                    except Exception as e:
                        self.logger.error(str(e))
                        raise e