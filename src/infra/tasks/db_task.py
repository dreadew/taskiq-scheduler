import traceback
import uuid
from typing import Any, List

from celery import shared_task
from sqlalchemy import text

from src.core.config import config
from src.core.enums import TaskStatus
from src.core.logging import get_logger
from src.core.utils.date import utc_now
from src.infra.db.sqlalchemy.models.entities import TaskExecution
from src.infra.db.sqlalchemy.session import SessionLocal, get_external_db_session_sync

logger = get_logger(__name__)


@shared_task(
    bind=True,
    max_retries=config.CELERY_TASK_MAX_RETRIES,
    acks_late=True,
    time_limit=config.CELERY_TASK_TIME_LIMIT,
)
def execute_db_task(
    self, execution_id: uuid.UUID, dsn: str, ddl: List[Any], queries: List[Any]
):
    """
    Фоновая задача для выполнения DDL и Queries
    :param execution_id: идентификатор запуска
    :param dsn: строка подключения к внешней БД
    :param ddl: запросы для создания структуры БД
    :param queries: запросы для проверки
    """

    logger.info(f"(фоновая задача {execution_id}): начинается обработка...")
    try:
        _run_task(execution_id, dsn, ddl, queries)
    except Exception as e:
        logger.exception(f"(фоновая задача {execution_id}): ошибка выполнения задачи")
        raise self.retry(exc=e, countdown=2**self.request.retries)


def _run_task(execution_id: uuid.UUID, dsn: str, ddl: List[Any], queries: List[Any]):
    """
    Запустить задачу на выполнение
    :param execution_id: идентификатор запуска
    :param dsn: строка подключения к внешней БД
    :param ddl: запросы для создания структуры БД
    :param queries: запросы для проверки
    """
    try:
        _save_task_startup(execution_id)
        results = _apply_ddl_and_queries(execution_id, dsn, ddl, queries)
        _save_task_result(execution_id, results)
    except Exception as e:
        logger.info(
            f"(фоновая задача {execution_id}): произошла ошибка при выполнении - {str(e)}"
        )
        tb = traceback.format_exc()
        error = {"error": str(e), "traceback": tb}
        _save_task_result(execution_id, [], error)
        raise
        raise


def _save_task_startup(execution_id: uuid.UUID):
    """
    Сохранить время начала выполнения задачи и изменить статус на RUNNING
    :param execution_id: идентификатор запуска
    """
    with SessionLocal() as session:
        logger.info(
            f"(фоновой задачи {execution_id}): сохранение информации о дате начала и смена статуса"
        )
        task_execution: TaskExecution = session.get(TaskExecution, execution_id)
        task_execution.started_at = utc_now()
        task_execution.status = TaskStatus.RUNNING
        session.commit()


def _apply_ddl_and_queries(
    execution_id: uuid.UUID, dsn: str, ddl: List[Any], queries: List[Any]
):
    """
    Применить ddl к внешней БД
    :param execution_id: идентификатор запущенной задачи
    :param dsn: строка подключения к БД
    :param ddl: запросы для создания схемы
    :param queries: запросы для выполнения в БД
    :return: результаты выполнения запросов
    """
    external_session = get_external_db_session_sync(dsn)
    results = []

    with external_session() as session:
        for query in ddl:
            logger.info(f"(фоновая задача {execution_id}): применение DDL ({query})")
            session.execute(text(query.rstrip(";")))

        for query in queries:
            logger.info(
                f"(фоновая задача {execution_id}): применение запроса ({query})"
            )
            result = session.execute(text(query["query"].rstrip(";")))

            query_result = {
                "queryid": query.get("queryid"),
                "query": query["query"],
                "rows": result.fetchall() if result.returns_rows else [],
                "rowcount": result.rowcount,
            }
            results.append(query_result)

        session.commit()

    return results


def _save_task_result(execution_id: uuid.UUID, results: List[Any], error=None):
    """
    Сохранить результат выполнения задачи
    :param execution_id: идентификатор запуска
    :param results: результат выполнения
    :param error: ошибка выполнения
    """
    if error is None:
        error = {}
    is_error = error is not None and error != {}

    logger.info(f"(фоновая задача {execution_id}): сохранение результатов")
    with SessionLocal() as session:
        task_execution: TaskExecution = session.get(TaskExecution, execution_id)
        task_execution.finished_at = utc_now()
        task_execution.status = TaskStatus.DONE if not is_error else TaskStatus.FAILED

        if not is_error:
            task_execution.result = {"success": True, "results": results}
        else:
            task_execution.result = error
            task_execution.attempt += 1

        session.commit()
