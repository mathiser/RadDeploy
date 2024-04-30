import time

from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped


def _now():
    return int(time.time())


class Base(DeclarativeBase):
    id: Mapped[int] = mapped_column(unique=True, primary_key=True, autoincrement=True)
    ts: Mapped[str] = mapped_column(default=_now)


class Row(Base):
    __tablename__ = "rows"
    UID: Mapped[str]
    Name: Mapped[str]
    Version: Mapped[str]
    Patient: Mapped[str]
    Priority: Mapped[int]
    Sender: Mapped[str]
    Destinations: Mapped[str]
    Received: Mapped[str] = mapped_column(default=_now)
    Dispatched: Mapped[str] = mapped_column(nullable=True, default=None)
    Finished: Mapped[str] = mapped_column(nullable=True, default=None)
    Sent: Mapped[str] = mapped_column(nullable=True, default=None)
    Status: Mapped[int] = mapped_column(default=0)


class Log(Base):
    __tablename__ = "logs"
    hostname: Mapped[str]
    levelname: Mapped[str]
    msg: Mapped[str]
    pathname: Mapped[str]
    funcName: Mapped[str]
    created: Mapped[int]

class ContainerLog(Base):
    __tablename__ = "container_logs"
    uid: Mapped[str]
    container_id: Mapped[str]
    hostname: Mapped[str]
    levelname: Mapped[str]
    msg: Mapped[str]
    pathname: Mapped[str]
    funcName: Mapped[str]
    created: Mapped[int]
