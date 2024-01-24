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
    flow_instance_uid: Mapped[str] = mapped_column(nullable=True, default=None)
    input_file_uid: Mapped[str]
    input_file_deleted: Mapped[bool] = mapped_column(default=False)
    output_file_uid: Mapped[str] = mapped_column(nullable=True, default=None)
    output_file_deleted: Mapped[bool] = mapped_column(default=False)
    exchange: Mapped[str]
    routing_key: Mapped[str]
    context_as_json: Mapped[str]


class DashboardRow(Base):
    __tablename__ = "dashboard_rows"
    flow_instance_uid: Mapped[str]
    flow_container_tag: Mapped[str]
    sender_ae_hostname: Mapped[str]
    status: Mapped[int] = mapped_column(default=0)
    dt_dispatched: Mapped[str] = mapped_column(nullable=True, default=None)
    dt_finished: Mapped[str] = mapped_column(nullable=True, default=None)
    dt_sent: Mapped[str] = mapped_column(nullable=True, default=None)
