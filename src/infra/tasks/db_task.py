import traceback
import uuid
from typing import Any, List

from sqlalchemy import text

from src.core.cancellation import CancellationContext, TaskCancelledError
from src.core.circuit_breaker import CircuitBreakerOpenError, circuit_breaker_protection
from src.core.config import config
from src.core.enums import TaskStatus
from src.core.logging import get_logger
from src.core.utils.date import utc_now
from src.infra.brokers.nats_broker import nats_broker as broker
from src.infra.db.sqlalchemy.models.entities import TaskExecution
from src.infra.db.sqlalchemy.session import AsyncSessionLocal, get_external_db_session
from src.infra.metrics.taskiq import (
    task_finished_metrics,
    task_retry_metrics,
    task_started_metrics,
)

logger = get_logger(__name__)


@broker.task(
    retry=config.TASKIQ_MAX_RETRIES,
    timeout=config.TASKIQ_TIME_LIMIT,
)
async def execute_db_task(
    execution_id: uuid.UUID, dsn: str, ddl: List[Any], queries: List[Any]
) -> None:
    """
    Фоновая задача для выполнения DDL и Queries
    :param execution_id: идентификатор запуска
    :param dsn: строка подключения к внешней БД
    :param ddl: запросы для создания структуры БД
    :param queries: запросы для проверки
    """

    task_id = str(execution_id)
    task_name = "execute_db_task"

    task_started_metrics(task_name, task_id)
    logger.info(f"(фоновая задача {execution_id}): начинается обработка...")

    try:
        async with CancellationContext(str(execution_id)) as ctx:
            await _run_task(execution_id, dsn, ddl, queries, ctx)
        task_finished_metrics(task_name, task_id, success=True)
    except TaskCancelledError:
        logger.info(f"(фоновая задача {execution_id}): задача отменена gracefully")
        task_finished_metrics(task_name, task_id, success=False, cancelled=True)
        await _update_execution_status(execution_id, TaskStatus.CANCELLED)
    except CircuitBreakerOpenError as e:
        logger.warning(f"(фоновая задача {execution_id}): circuit breaker открыт - {e}")
        task_finished_metrics(task_name, task_id, success=False)
        await _update_execution_status(execution_id, TaskStatus.FAILED)
        return
    except Exception as e:
        logger.exception(f"(фоновая задача {execution_id}): ошибка выполнения задачи")
        task_retry_metrics(task_name)
        raise e


async def _update_execution_status(execution_id: uuid.UUID, status: TaskStatus):
    """Обновить статус выполнения задачи"""
    async with AsyncSessionLocal() as session:
        execution = await session.get(TaskExecution, execution_id)
        if execution:
            execution.status = status
            await session.commit()


async def _run_task(
    execution_id: uuid.UUID, dsn: str, ddl: List[Any], queries: List[Any], ctx
):
    """
    Запустить задачу на выполнение с поддержкой graceful отмены
    :param execution_id: идентификатор запуска
    :param dsn: строка подключения к внешней БД
    :param ddl: запросы для создания структуры БД
    :param queries: запросы для проверки
    :param ctx: контекст отмены
    """
    try:
        await _save_task_startup(execution_id)

        if await ctx.is_cancelled():
            raise TaskCancelledError()

        results = await _apply_ddl_and_queries(execution_id, dsn, ddl, queries, ctx)
        await _save_task_result(execution_id, results)
    except TaskCancelledError:
        raise
    except Exception as e:
        logger.info(
            f"(фоновая задача {execution_id}): произошла ошибка при выполнении - {str(e)}"
        )
        tb = traceback.format_exc()
        error = {"error": str(e), "traceback": tb}
        await _save_task_result(execution_id, [], error)
        raise


async def _save_task_startup(execution_id: uuid.UUID):
    """
    Сохранить время начала выполнения задачи и изменить статус на RUNNING
    :param execution_id: идентификатор запуска
    """
    async with AsyncSessionLocal() as session:
        logger.info(
            f"(фоновой задачи {execution_id}): сохранение информации о дате начала и смена статуса"
        )
        task_execution: TaskExecution = await session.get(TaskExecution, execution_id)
        task_execution.started_at = utc_now()
        task_execution.status = TaskStatus.RUNNING
        await session.commit()


async def _apply_ddl_and_queries(
    execution_id: uuid.UUID, dsn: str, ddl: List[Any], queries: List[Any], ctx
):
    """
    Применить ddl к внешней БД с поддержкой graceful отмены
    :param execution_id: идентификатор запущенной задачи
    :param dsn: строка подключения к БД
    :param ddl: запросы для создания схемы
    :param queries: запросы для выполнения в БД
    :param ctx: контекст отмены
    :return: результаты выполнения запросов
    """
    external_session = get_external_db_session(dsn)
    results = []

    async with circuit_breaker_protection(dsn):
        async with external_session() as session:
            for ddl_stmt in ddl:
                if await ctx.is_cancelled():
                    raise TaskCancelledError()

                logger.info(
                    f"(фоновая задача {execution_id}): применение DDL ({ddl_stmt['statement']})"
                )
                await session.execute(text(ddl_stmt["statement"].rstrip(";")))

            for query_item in queries:
                if await ctx.is_cancelled():
                    raise TaskCancelledError()

                logger.info(
                    f"(фоновая задача {execution_id}): применение запроса ({query_item['query']})"
                )
                result = await session.execute(text(query_item["query"].rstrip(";")))

                rows_data = []
                if result.returns_rows:
                    rows = result.fetchall()
                    rows_data = [tuple(row) for row in rows]

                query_result = {
                    "queryid": query_item["queryid"],
                    "query": query_item["query"],
                    "rows": rows_data,
                    "rowcount": result.rowcount,
                }
                results.append(query_result)

            await session.commit()

    return results


async def _save_task_result(execution_id: uuid.UUID, results: List[Any], error=None):
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
    async with AsyncSessionLocal() as session:
        task_execution: TaskExecution = await session.get(TaskExecution, execution_id)
        task_execution.finished_at = utc_now()
        task_execution.status = TaskStatus.DONE if not is_error else TaskStatus.FAILED

        if not is_error:
            task_execution.result = {"success": True, "results": results}
        else:
            task_execution.result = error
            task_execution.attempt += 1

        await session.commit()
