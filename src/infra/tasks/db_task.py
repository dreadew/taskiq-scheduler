import traceback
import uuid
from typing import Any, List

from src.core.cancellation import CancellationContext, TaskCancelledError
from src.core.circuit_breaker import CircuitBreakerOpenError
from src.core.config import config
from src.core.enums import TaskStatus
from src.core.logging import get_logger
from src.core.utils.date import utc_now
from src.infra.brokers.nats_broker import nats_broker as broker
from src.infra.clients.grpc_client import schema_review_client
from src.infra.db.sqlalchemy.models.entities import TaskExecution
from src.infra.db.sqlalchemy.session import AsyncSessionLocal
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
    Фоновая задача для анализа DDL и Queries через gRPC сервис
    :param execution_id: идентификатор запуска
    :param dsn: URL базы данных (для передачи в gRPC сервис)
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
    Запустить задачу анализа через gRPC с поддержкой graceful отмены
    :param execution_id: идентификатор запуска
    :param dsn: URL базы данных для передачи в gRPC сервис
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
    Отправить DDL и queries в gRPC сервис для анализа схемы
    :param execution_id: идентификатор запущенной задачи
    :param dsn: строка подключения к БД (используется как URL)
    :param ddl: запросы для создания схемы
    :param queries: запросы для выполнения в БД
    :param ctx: контекст отмены
    :return: результаты анализа от gRPC сервиса
    """
    logger.info(
        f"(фоновая задача {execution_id}): отправка данных в gRPC сервис для анализа"
    )

    if await ctx.is_cancelled():
        raise TaskCancelledError()

    ddl_statements = [ddl_stmt["statement"] for ddl_stmt in ddl]

    grpc_queries = []
    for query_item in queries:
        grpc_query = {
            "query_id": query_item.get("queryid", ""),
            "query": query_item.get("query", ""),
            "runquantity": query_item.get("runquantity", 0),
            "executiontime": query_item.get("executiontime", 0),
        }
        grpc_queries.append(grpc_query)

    try:
        async with schema_review_client as client:
            if await ctx.is_cancelled():
                raise TaskCancelledError()

            logger.info(
                f"(фоновая задача {execution_id}): отправка {len(ddl_statements)} DDL и {len(grpc_queries)} запросов"
            )

            grpc_result = await client.review_schema(
                url=dsn,
                ddl_statements=ddl_statements,
                queries=grpc_queries,
                thread_id=str(execution_id),
            )

            logger.info(
                f"(фоновая задача {execution_id}): получен ответ от gRPC сервиса: success={grpc_result['success']}"
            )

            if grpc_result.get("ddl"):
                for ddl_result in grpc_result["ddl"]:
                    logger.info(f"DDL анализ: {ddl_result['statement']}")

            if grpc_result.get("migrations"):
                for migration in grpc_result["migrations"]:
                    logger.info(f"Рекомендованная миграция: {migration['statement']}")

            if grpc_result.get("warnings"):
                for warning in grpc_result["warnings"]:
                    logger.warning(f"Предупреждение анализа: {warning}")

            if grpc_result.get("error"):
                logger.error(f"Ошибка анализа: {grpc_result['error']}")
                raise Exception(
                    f"gRPC анализ завершился с ошибкой: {grpc_result['error']}"
                )

            # Возвращаем весь результат gRPC для сохранения в БД
            return grpc_result

    except Exception as e:
        logger.error(
            f"(фоновая задача {execution_id}): ошибка при обращении к gRPC сервису: {e}"
        )
        raise


async def _save_task_result(execution_id: uuid.UUID, results: Any, error=None):
    """
    Сохранить результат выполнения задачи
    :param execution_id: идентификатор запуска
    :param results: результат анализа от gRPC сервиса
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
            task_execution.result = {
                "ddl": results.get("ddl", []),
                "migrations": results.get("migrations", []),
                "queries": results.get("queries", []),
            }
        else:
            task_execution.result = error
            task_execution.attempt += 1

        await session.commit()
