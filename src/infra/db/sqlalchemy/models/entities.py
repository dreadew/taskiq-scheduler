from sqlalchemy import (
    JSON,
    UUID,
    CheckConstraint,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.orm import relationship

from src.core.enums import TaskStatus
from src.infra.db.sqlalchemy.models.base import AuditableEntity


class Task(AuditableEntity):
    """
    Фоновая задача
    """

    __tablename__ = "tasks"

    default_priority = Column(Integer)

    __table_args__ = (
        CheckConstraint(
            "default_priority >= 0 AND default_priority <= 9", "chk_default_priority"
        ),
    )


class TaskExecution(AuditableEntity):
    """
    Экземпляр фоновой задачи
    """

    __tablename__ = "task_executions"

    task_id = Column(
        UUID(as_uuid=True), ForeignKey("tasks.id", ondelete="RESTRICT"), nullable=False
    )
    celery_task_id = Column(String(36), unique=True, nullable=True)
    parameters = Column(JSON)
    scheduled_at = Column(DateTime(timezone=True), nullable=True)
    started_at = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    status = Column(Enum(TaskStatus), default=TaskStatus.SCHEDULED)
    priority = Column(Integer, default=5)
    attempt = Column(Integer, default=0)
    result = Column(JSON)
    prev_execution_id = Column(
        UUID(as_uuid=True), ForeignKey("task_executions.id"), nullable=True
    )

    task = relationship("Task")
    prev_execution = relationship("TaskExecution")

    __table_args__ = (
        CheckConstraint("priority >= 0 AND priority <= 9", "chk_priority"),
    )
