import datetime

from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped


class Base(DeclarativeBase):
    id: Mapped[int] = mapped_column(unique=True, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime.datetime] = mapped_column(default=datetime.datetime.now)


class Event(Base):
    __tablename__ = "events"
    uid: Mapped[str]
    flow_instance_uid: Mapped[str] = mapped_column(nullable=True, default=None)
    flow_name: Mapped[str] = mapped_column(nullable=True, default=None)
    input_file_uid: Mapped[str]
    input_file_deleted: Mapped[bool] = mapped_column(default=False)
    output_file_uid: Mapped[str] = mapped_column(nullable=True, default=None)
    output_file_deleted: Mapped[bool] = mapped_column(default=False)
    exchange: Mapped[str]
    routing_key: Mapped[str]
    context_as_json: Mapped[str]
    sender_ae_hostname:  Mapped[str]
    sender_ae_port:  Mapped[int]
    sender_ae_title: Mapped[str]

