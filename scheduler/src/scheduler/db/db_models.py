import time

from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped


def _now():
    return int(time.time())


class Base(DeclarativeBase):
    id: Mapped[int] = mapped_column(unique=True, primary_key=True, autoincrement=True)
    ts: Mapped[str] = mapped_column(default=_now)


class MountMapping(Base):
    __tablename__ = "mount_mappings"
    __table_args__ = (UniqueConstraint('uid', 'name', 'file_uid', name='_mount_mapping_uc'), )
    uid: Mapped[str]  # Flow UID
    name: Mapped[str]  # Human readable names of volumes
    file_uid: Mapped[str]  # UID used when collecting file from file_storage


class DispatchedModel(Base):
    __tablename__ = "dispatched_models"
    __table_args__ = (UniqueConstraint('uid', 'model_id', name='_dispatched_model_uc'), )
    uid: Mapped[str]  # Flow UID
    model_id: Mapped[int]
