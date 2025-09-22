from typing import Any, List, Optional
from uuid import UUID

from src.core.cancellation import cancellation_registry
from src.core.config import config
from src.core.enums import TaskStatus
from src.core.logging import get_logger
from src.core.utils.date import utc_now
from src.core.utils.json import json_serialize
from src.core.utils.parse_url import parse_dsn
from src.core.utils.sql_validator import SQLValidatorFactory, validate_sql_batch
from src.infra.db.sqlalchemy.models.entities import Task, TaskExecution
from src.infra.queues.taskiq_task_queue import TaskQueue
from src.infra.repos.base_repo import BaseRepository


class TaskService:
    """
    Сервис для работы с репозиторием фоновых задач
    """

    def __init__(
        self,
        task_repo: BaseRepository[Task],
        task_execution_repo: BaseRepository[TaskExecution],
        task_queue: TaskQueue,
    ):
        self.task_repo = task_repo
        self.task_execution_repo = task_execution_repo
        self.task_queue = task_queue
        self.logger = get_logger(__name__)

    async def get_task_execution(self, execution_id) -> TaskExecution | None:
        """
        Получить информацию о запущенной задаче по идентификатору
        :param execution_id: идентификатор запущенной задачи
        :return: запущенная задачи
        """
        self.logger.info(f"получение информации о запущенной задаче: {execution_id}")
        return await self.task_execution_repo.get(execution_id)

    async def enqueue_task(
        self, dsn: str, ddl: List[Any], queries: List[Any], priority: Optional[int] = 3
    ):
        """
        Создание записи о запуске задачи и постановка ее в очередь Taskiq
        :param dsn: строка подключения к внешней БД
        :param ddl: запросы для создания структуры БД
        :param queries: запросы к БД
        :param priority: приоритет задачи
        :return: идентификатор запущенной задачи
        """

        parse_dsn(dsn)
        if priority > 9 or priority < 0:
            raise ValueError("приоритет не может быть больше 9 или меньше 0.")

        self._validate_sql_queries(dsn, ddl, queries)

        task_obj = Task(default_priority=config.TASKIQ_DEFAULT_PRIORITY)
        task_id = await self.task_repo.create(task_obj)
        self.logger.info(f"создана задача {task_id}")

        prev_execution = await self.task_execution_repo.find_latest_by_field(
            "task_id", task_id
        )

        params = {
            "dsn": dsn,
            "ddl": [d.model_dump() for d in ddl],
            "queries": [q.model_dump() for q in queries],
        }

        execution_obj = TaskExecution(
            task_id=task_id,
            parameters=json_serialize(params),
            scheduled_at=utc_now(),
            status=TaskStatus.SCHEDULED,
            priority=(
                priority if priority is not None else config.TASKIQ_DEFAULT_PRIORITY
            ),
            prev_execution_id=prev_execution.id if prev_execution else None,
        )
        execution_id = await self.task_execution_repo.create(execution_obj)
        self.logger.info(f"создан execution {execution_id} для задачи {task_id}")
        self.logger.info(f"NATS CFG: {config.NATS_URL} {config.NATS_HOST}")

        params["execution_id"] = str(execution_id)
        queued_task = await self.task_queue.queue_task(params)
        self.logger.info(
            f"отправлена в очередь taskiq-задача {queued_task.task_id} для execution {execution_id}"
        )

        await self.task_execution_repo.update(
            execution_id, {"broker_task_id": str(queued_task.task_id)}
        )

        return execution_id

    async def cancel(self, execution_id: UUID):
        """
        Отменить выполнение задачи с транзакционностью и graceful shutdown
        :param execution_id: идентификатор запущенной задачи
        """

        async with self.task_execution_repo.transaction():
            execution = await self.task_execution_repo.get_for_update(execution_id)
            if execution is None or execution.broker_task_id is None:
                raise ValueError("задача не найдена.")

            if execution.status not in {TaskStatus.SCHEDULED, TaskStatus.RUNNING}:
                raise ValueError(
                    "остановить можно задачу только в статусах SCHEDULED или RUNNING."
                )

            try:
                await cancellation_registry.cancel_task(str(execution_id))

                await self.task_execution_repo.update(
                    execution_id, {"status": TaskStatus.CANCELLING}
                )

                await self.task_queue.cancel_task(execution.broker_task_id)

                await self.task_execution_repo.update(
                    execution_id, {"status": TaskStatus.CANCELLED}
                )

                self.logger.info(f"Задача {execution_id} успешно отменена")

            except Exception as e:
                self.logger.error(f"Ошибка при отмене задачи {execution_id}: {e}")
                await self.task_execution_repo.update(
                    execution_id, {"status": TaskStatus.STOPPED}
                )
                raise

    def _validate_sql_queries(self, dsn: str, ddl: List[Any], queries: List[Any]):
        """
        Валидация SQL запросов перед выполнением.

        :param dsn: строка подключения к БД
        :param ddl: список DDL запросов
        :param queries: список DML запросов
        :raises ValueError: если валидация не прошла
        """
        try:
            validator = SQLValidatorFactory.create_validator_from_dsn(dsn)

            if ddl:
                ddl_statements = [d.statement for d in ddl]
                ddl_results = validate_sql_batch(
                    ddl_statements, validator.dialect, is_ddl=True
                )

                for i, result in enumerate(ddl_results):
                    if not result.is_valid:
                        self.logger.error(
                            f"DDL валидация не прошла для запроса {i+1}: {result.errors}"
                        )
                        raise ValueError(
                            f"DDL запрос {i+1} содержит ошибки: {'; '.join(result.errors)}"
                        )

                    if result.warnings:
                        self.logger.warning(
                            f"DDL предупреждения для запроса {i+1}: {result.warnings}"
                        )

            if queries:
                query_statements = [q.query for q in queries]
                query_results = validate_sql_batch(
                    query_statements, validator.dialect, is_ddl=False
                )

                for i, result in enumerate(query_results):
                    if not result.is_valid:
                        self.logger.error(
                            f"Query валидация не прошла для запроса {i+1}: {result.errors}"
                        )
                        raise ValueError(
                            f"Query {i+1} содержит ошибки: {'; '.join(result.errors)}"
                        )

                    if result.warnings:
                        self.logger.warning(
                            f"Query предупреждения для запроса {i+1}: {result.warnings}"
                        )

            self.logger.info(
                f"SQL валидация прошла успешно: {len(ddl)} DDL, {len(queries)} queries"
            )

        except Exception as e:
            self.logger.error(f"Ошибка валидации SQL: {str(e)}")
            raise
