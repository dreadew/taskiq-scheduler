import asyncio
from contextlib import asynccontextmanager
from typing import Set

from src.core.logging import get_logger

logger = get_logger(__name__)


class TaskCancelledError(Exception):
    """Исключение, выбрасываемое при отмене задачи"""

    pass


class CancellationRegistry:
    """Реестр отмененных задач"""

    def __init__(self):
        self._cancelled_tasks: Set[str] = set()
        self._lock = asyncio.Lock()

    async def cancel_task(self, execution_id: str):
        """Отметить задачу как отмененную"""
        async with self._lock:
            self._cancelled_tasks.add(execution_id)
            logger.info(f"Задача {execution_id} отмечена для отмены")

    async def is_cancelled(self, execution_id: str) -> bool:
        """Проверить, отменена ли задача"""
        async with self._lock:
            return execution_id in self._cancelled_tasks

    async def remove_task(self, execution_id: str):
        """Удалить задачу из реестра (после завершения)"""
        async with self._lock:
            self._cancelled_tasks.discard(execution_id)


cancellation_registry = CancellationRegistry()


@asynccontextmanager
async def CancellationContext(execution_id: str):
    """
    Контекст для graceful отмены задач

    Использование:
    async with CancellationContext(execution_id) as ctx:
        for operation in operations:
            if ctx.is_cancelled():
                raise TaskCancelledError()
            await operation()
    """

    class Context:
        def __init__(self, execution_id: str):
            self.execution_id = execution_id

        async def is_cancelled(self) -> bool:
            return await cancellation_registry.is_cancelled(self.execution_id)

        def check_cancellation(self):
            """Синхронная проверка отмены с выбросом исключения"""
            if asyncio.iscoroutinefunction(cancellation_registry.is_cancelled):
                pass

    ctx = Context(execution_id)
    try:
        yield ctx
    finally:
        await cancellation_registry.remove_task(execution_id)
