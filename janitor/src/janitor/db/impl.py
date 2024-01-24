import os

import sqlalchemy
from sqlalchemy.orm import sessionmaker, scoped_session, session

from DicomFlowLib.data_structures.contexts import FlowContext
from DicomFlowLib.fs import FileStorage
from .db_models import Base, Event, DashboardRow, _now


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

    def delete_files_by_id(self, id):
        event = self.get_objs_by_kwargs(id=id).first()
        try:
            if not event.input_file_deleted:
                self.fs.delete(event.input_file_uid)
                self.update_event(id=event.id, input_file_deleted=True)
        except FileNotFoundError:
            self.update_event(id=event.id, input_file_deleted=True)
        except Exception as e:
            raise e

        ## Output file
        try:
            if event.output_file_uid:
                if not event.output_file_deleted:
                    self.fs.delete(event.output_file_uid)
                    self.update_event(id=event.id, output_file_deleted=True)
        except FileNotFoundError:
            self.update_event(id=event.id, output_file_deleted=True)
        except Exception as e:
            raise e

    def delete_all_files_by_kwargs(self, **kwargs):
        all_events_by_uid = self.get_objs_by_kwargs(**kwargs).all()
        for event in all_events_by_uid:
            try:
                self.delete_files_by_id(id=event.id)
            except FileNotFoundError:
                pass

    def maybe_insert_dashboard_row(self,
                             flow_instance_uid: str,
                             flow_container_tag: str,
                             sender_ae_hostname: str):
        with self.Session() as session:
            row = session.query(DashboardRow).filter_by(flow_instance_uid=flow_instance_uid).first()
            if not row:
                row = DashboardRow(flow_instance_uid=flow_instance_uid,
                                   flow_container_tag=flow_container_tag,
                                   sender_ae_hostname=sender_ae_hostname)
                session.add(row)
                session.commit()
                session.refresh(row)
                return row
            else:
                return row

    def set_status_of_dashboard_row(self, flow_instance_uid: str, status: int):
        with (self.Session() as session):
            row = session.query(DashboardRow).filter_by(flow_instance_uid=flow_instance_uid).first()
            assert row

            row.status = status
            if status == 0:
                pass
            elif status == 1:
                row.dt_dispatched = _now()
            elif status == 2:
                row.dt_finished = _now()
            elif status == 3:
                row.dt_sent = _now()
            elif status == 400:
                pass
            else:
                raise Exception("Invalid Status")

            session.commit()
            session.refresh(row)
            return row