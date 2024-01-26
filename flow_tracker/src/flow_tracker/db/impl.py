import os

import sqlalchemy
from sqlalchemy.orm import sessionmaker, scoped_session

from .db_models import Base, Row, _now


class Database:
    def __init__(self, database_path: str):
        self.database_path = database_path
        os.makedirs(os.path.dirname(self.database_path), exist_ok=True)

        self.database_url = f'sqlite:///{self.database_path}'
        self.engine = sqlalchemy.create_engine(self.database_url, future=True)

        # Check if database exists - if not, create scheme
        if not os.path.isfile(self.database_path):
            Base.metadata.create_all(self.engine)

        self.session_maker = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.Session = scoped_session(self.session_maker)


    def maybe_insert_row(self,
                         uid: str,
                         name: str,
                         sender: str,
                         priority: int):
        with self.Session() as session:
            row = session.query(Row).filter_by(UID=uid).first()
            if not row:
                row = Row(UID=uid,
                          Name=name,
                          Sender=sender,
                          Priority=priority)
                session.add(row)
                session.commit()
                session.refresh(row)
                return row
            else:
                return row

    def set_status_of_row(self, uid: str, status: int):
        with self.Session() as session:
            row = session.query(Row).filter_by(UID=uid).first()
            assert row

            if status == 0:
                pass
            elif status == 1:
                row.Dispatched = _now()
            elif status == 2:
                row.Finished = _now()
            elif status == 4:
                row.Sent = _now()
            elif status == 400:
                pass

            if status > row.Status:
                row.Status = status

            session.commit()
            session.refresh(row)
            return row
