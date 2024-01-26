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
    Priority: Mapped[int]
    Sender: Mapped[str]
    Dispatched: Mapped[str] = mapped_column(nullable=True, default=None)
    Finished: Mapped[str] = mapped_column(nullable=True, default=None)
    Sent: Mapped[str] = mapped_column(nullable=True, default=None)
    Status: Mapped[int] = mapped_column(default=0)
