from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from src.core.config import config


class DDLStatement(BaseModel):
    """
    Схема DDL
    """

    statement: str

    @field_validator("statement")
    @classmethod
    def validate_ddl_statement(cls, v: str) -> str:
        """Базовая валидация DDL statement."""
        if not v or not v.strip():
            raise ValueError("DDL statement не может быть пустым")

        dangerous_keywords = ["DROP DATABASE", "DROP SCHEMA", "DROP USER", "DROP ROLE"]
        v_upper = v.upper().strip()

        for keyword in dangerous_keywords:
            if keyword in v_upper:
                raise ValueError(f"Запрещенная DDL операция: {keyword}")

        return v.strip()


class QueryItem(BaseModel):
    """
    Схема Query
    """

    queryid: Optional[UUID]
    query: str
    runquantity: Optional[int]
    executiontime: Optional[int]

    @field_validator("query")
    @classmethod
    def validate_query(cls, v: str) -> str:
        """Базовая валидация SQL query."""
        if not v or not v.strip():
            raise ValueError("Query не может быть пустым")

        dangerous_keywords = ["DROP DATABASE", "DROP SCHEMA", "DROP USER", "TRUNCATE"]
        v_upper = v.upper().strip()

        for keyword in dangerous_keywords:
            if keyword in v_upper:
                raise ValueError(f"Запрещенная операция в query: {keyword}")

        return v.strip()


class TaskRunRequest(BaseModel):
    """
    Схема для запуска задачи на проверку БД
    """

    url: str
    ddl: List[DDLStatement] = Field(default_factory=list)
    queries: List[QueryItem] = Field(default_factory=list)
    priority: Optional[int] = Field(default=config.TASKIQ_DEFAULT_PRIORITY, ge=0, le=9)

    @classmethod
    @field_validator("url")
    def validate_url(cls, v):
        if not (v.startswith("jdbc:") or v.startswith("postgresql:")):
            raise ValueError("url должен начинаться с 'jdbc:' или с 'postgresql:'")
        return v


class TaskRunResponse(BaseModel):
    """
    Ответ на запрос о запуске задачи на проверку БД
    """

    execution_id: UUID
    status: str


class TaskStatusResponse(BaseModel):
    """
    Ответ на запрос о статусе задачи
    """

    status: str


class TaskResultResponse(BaseModel):
    """
    Ответ на запрос о результате задачи
    """

    result: Optional[dict] = {}


class TaskStopRequest(BaseModel):
    """
    Запрос для остановки выполнения задачи
    """

    execution_id: UUID
