import os

import sqlalchemy
from sqlalchemy.orm import sessionmaker, scoped_session

from .db_models import Base, Row, _now


class Database:
    def __init__(self, logger, database_path: str):
        self.database_path = database_path
        os.makedirs(os.path.dirname(self.database_path), exist_ok=True)

        self.database_url = f'sqlite:///{self.database_path}'
        self.engine = sqlalchemy.create_engine(self.database_url, future=True)

        # Check if database exists - if not, create scheme
        if not os.path.isfile(self.database_path):
            Base.metadata.create_all(self.engine)

        self.session_maker = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.Session = scoped_session(self.session_maker)

        self.logger = logger
    def maybe_insert_row(self,
                         uid: str,
                         name: str,
                         version: str,
                         patient: str,
                         sender: str,
                         priority: int,
                         destinations: str):

        with self.Session() as session:
            row = session.query(Row).filter_by(UID=uid).first()
            if not row:
                row = Row(UID=uid,
                          Name=name,
                          Patient=patient,
                          Sender=sender,
                          Priority=priority,
                          Destinations=destinations,
                          Version=version)
                session.add(row)
                session.commit()
                session.refresh(row)
                self.logger.info(f"Inserted row: {row.__dict__}")
                return row
            else:
                return row

    def set_status_of_row(self, uid: str, status: int):
        with self.Session() as session:
            row = session.query(Row).filter_by(UID=uid).first()
            assert row

            if status == 0:
                pass
            elif status == 2:
                if not row.Dispatched:
                    row.Dispatched = _now()
            elif status == 3:
                row.Finished = _now()
            elif status == 5:
                row.Sent = _now()
            elif status == 400:
                pass

            if status > row.Status:
                row.Status = status

            session.commit()
            session.refresh(row)
            return row
