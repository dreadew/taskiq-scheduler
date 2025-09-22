import uuid

from sqlalchemy import Column, DateTime, Integer
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base

from src.core.utils.date import utc_now

Base = declarative_base()


class BaseEntity(Base):
    """
    Базовая сущность
    """

    __abstract__ = True

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        index=True,
        default=uuid.uuid4,
    )


class AuditableEntity(BaseEntity):
    """
    Сущность с аудитом
    """

    __abstract__ = True

    created_at = Column(DateTime(timezone=True), default=utc_now)
    updated_at = Column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)
