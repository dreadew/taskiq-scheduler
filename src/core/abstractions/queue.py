from abc import ABC, abstractmethod

from src.core.config import config


class Queue(ABC):
    """
    Базовый класс очереди
    """

    @abstractmethod
    def queue_task(
        self,
        task_name: str,
        priority: int = config.CELERY_TASK_DEFAULT_PRIORITY,
        params=None,
    ):
        """
        Поставить задачу в очередь
        :param task_name: название задачи
        :param priority: приоритет
        :param params: параметры для задачи
        """
        pass
