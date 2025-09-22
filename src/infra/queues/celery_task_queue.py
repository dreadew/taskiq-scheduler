from uuid import UUID

from src.core.abstractions.queue import Queue
from src.core.config import config
from src.infra.scheduler import celery_app


class TaskQueue(Queue):
    def __init__(self):
        self.celery_app = celery_app

    def queue_task(
        self,
        task_name: str,
        priority: int = config.CELERY_TASK_DEFAULT_PRIORITY,
        params=None,
    ):
        """
        Добавить задачу в очередь
        :param task_name: название задачи
        :param priority: приоритет
        :param params: параметры задачи
        :return: AsyncResult
        """
        if params is None:
            params = {}
        return self.celery_app.send_task(task_name, kwargs=params, priority=priority)

    def cancel_task(self, task_id: UUID):
        """
        Отменить выполнение задачи
        :param task_id: идентификатор задачи
        """
        self.celery_app.revoke(task_id, terminate=True)
