from abc import ABC, abstractmethod


class Queue(ABC):
    """
    Базовый класс очереди
    """

    @abstractmethod
    async def queue_task(self, params=None):
        """
        Поставить задачу в очередь
        :param params: параметры для задачи
        """
        pass

    @abstractmethod
    async def cancel_task(self, task_id: str):
        """
        Отменить выполнение задачи
        :param task_id: идентификатор задачи
        """
        pass
