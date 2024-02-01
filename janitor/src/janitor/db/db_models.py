import time

from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped


def _now():
    return time.time()


class Base(DeclarativeBase):
    id: Mapped[int] = mapped_column(unique=True, primary_key=True, autoincrement=True)
    timestamp: Mapped[str] = mapped_column(default=_now)


class Event(Base):
    __tablename__ = "events"
    uid: Mapped[str]
    flow_instance_uid: Mapped[str] = mapped_column(nullable=True, default="")
    input_file_uid: Mapped[str]
    input_file_deleted: Mapped[bool] = mapped_column(default=False)
    output_file_uid: Mapped[str] = mapped_column(default="")
    output_file_deleted: Mapped[bool] = mapped_column(default=False)
    exchange: Mapped[str]
    routing_key: Mapped[str]