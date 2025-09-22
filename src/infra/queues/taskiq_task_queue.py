from typing import Dict

from taskiq import TaskiqResult

from src.core.abstractions.queue import Queue
from src.core.logging import get_logger
from src.infra.tasks.db_task import execute_db_task

logger = get_logger(__name__)


class TaskQueue(Queue):
    def __init__(self):
        self.broker = None
        self._running_tasks: Dict[str, TaskiqResult] = {}

    async def queue_task(self, params=None) -> TaskiqResult:
        """
        Добавить задачу в очередь
        :param params: параметры задачи
        :return: TaskiqResult
        """
        if params is None:
            params = {}

        execution_id = params["execution_id"]

        result = await execute_db_task.kiq(
            execution_id=execution_id,
            dsn=params["dsn"],
            ddl=params["ddl"],
            queries=params["queries"],
        )

        self._running_tasks[str(execution_id)] = result
        logger.info(
            f"Задача {execution_id} добавлена в очередь с task_id: {result.task_id}"
        )

        return result

    async def cancel_task(self, task_id: str):
        """
        Отменить выполнение задачи
        :param task_id: идентификатор задачи (execution_id)
        """
        if task_id in self._running_tasks:
            task_result = self._running_tasks[task_id]
            try:
                if not task_result.is_done():
                    logger.info(f"Попытка отмены задачи {task_id}")
                    del self._running_tasks[task_id]
                    logger.warning(
                        f"Задача {task_id} удалена из локального отслеживания"
                    )
                else:
                    logger.info(f"Задача {task_id} уже завершена")
            except Exception as e:
                logger.error(f"Ошибка при отмене задачи {task_id}: {e}")
        else:
            logger.warning(f"Задача {task_id} не найдена для отмены")
